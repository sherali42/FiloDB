package filodb.core.query

import java.util.Objects

import scala.reflect.runtime.universe._

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import net.ceedubs.ficus.Ficus._
import org.joda.time.DateTime

import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType
import filodb.core.store.ChunkScanMethod
import filodb.memory.format.RowReader

/**
 * Some basic info about a single Partition
 */
final case class PartitionInfo(schema: RecordSchema, base: Array[Byte], offset: Long, shardNo: Int) {
  def partKeyBytes: Array[Byte] = schema.asByteArray(base, offset)
  override def toString: String = s"/shard:$shardNo/${schema.stringify(base, offset)}"
}

/**
 * Describes column/field name and type.
 * isCumulative is not considered for equality/Hashcode.
 */
final case class ColumnInfo(name: String, colType: Column.ColumnType, isCumulative: Boolean = true) {
  override def equals(obj: Any): Boolean = obj match {
    case ColumnInfo(n: String, ct: Column.ColumnType, _) => n == name && ct == colType
    case _ => false
  }
  override def hashCode(): Int = Objects.hash(name, colType)
}

object ColumnInfo {
  def apply(col: Column): ColumnInfo = ColumnInfo(col.name, col.columnType, isCumulative(col))
  private def isCumulative(col: Column): Boolean =
    col.params.as[Option[Boolean]]("detectDrops").getOrElse(false) ||
      col.params.as[Option[Boolean]]("counter").getOrElse(false)
}

/**
 * Describes the full schema of result types, including how many initial columns are for row keys.
 * The first ColumnInfo in the schema describes the first vector in Vectors and first field in Tuples, etc.
 * @param brSchemas if any of the columns is a BinaryRecord: map of colNo -> inner BinaryRecord schema
 * @param numRowKeyColumns the number of row key columns at the start of columnns
 * @param fixedVectorLen if defined, each vector is guaranteed to have exactly this many output elements.
 *                       See PeriodicSampleMapper for an example of how this is used.
 * @param colIDs the column IDs of the columns, used to access additional columns if needed
 */
final case class ResultSchema(columns: Seq[ColumnInfo], numRowKeyColumns: Int,
                              brSchemas: Map[Int, RecordSchema] = Map.empty,
                              fixedVectorLen: Option[Int] = None,
                              colIDs: Seq[Int] = Nil) {
  import Column.ColumnType._

  def length: Int = columns.length
  def isEmpty: Boolean = columns.isEmpty
  def isTimeSeries: Boolean = columns.nonEmpty && numRowKeyColumns == 1 &&
                              (columns.head.colType == LongColumn || columns.head.colType == TimestampColumn)
  // True if main col is Histogram and extra column is a Double
  def isHistDouble: Boolean = columns.length == 3 &&
                              columns(1).colType == HistogramColumn && columns(2).colType == DoubleColumn
  def isHistogram: Boolean = columns.length == 2 && columns(1).colType == HistogramColumn
  def isAvgAggregator: Boolean = columns.length == 3 && columns(2).name.equals("count")
  def isStdValAggregator: Boolean = columns.length == 4 && columns(2).name.equals("mean")

  def hasSameColumnsAs(other: ResultSchema): Boolean = {
    // exclude fixedVectorLen & colIDs
    other.columns == columns && other.numRowKeyColumns == numRowKeyColumns &&
      other.brSchemas == brSchemas
  }

  def hasSameColumnTypes(other: ResultSchema): Boolean = {
    // exclude column names
    other.columns.map(_.colType) == columns.map(_.colType)
  }
}

object ResultSchema {
  val empty = ResultSchema(Nil, 1)

  def valueColumnType(schema: ResultSchema): ColumnType = {
    require(schema.isTimeSeries, s"Schema $schema is not time series based, cannot continue query")
    require(schema.columns.size >= 2, s"Schema $schema has less than 2 columns, cannot continue query")
    schema.columns(1).colType
  }
}

/**
 * There are three types of final query results.
 * - a list of raw (or via function, transformed) time series samples, with an optional key range
 * - a list of aggregates
 * - a final aggregate
 */
// NOTE: the Serializable is needed for Akka to choose a more specific serializer (eg Kryo)
sealed trait Result extends java.io.Serializable {
  def schema: ResultSchema

  /**
   * Returns an Iterator of (Option[PartitionInfo], Seq[RowReader]) which helps with serialization. Basically each
   * element of the returned Seq contains partition info (optional), plus a Seq of RowReaders.  Each RowReader
   * can then be converted to pretty print text, JSON, etc. etc.
   */
  def toRowReaders: Iterator[(Option[PartitionInfo], Seq[RowReader])]

  /**
   * Pretty prints all the elements into strings.  Returns an iterator to avoid memory bloat.
   */
  def prettyPrint(formatTime: Boolean = true, partitionRowLimit: Int = 50): Iterator[String] = {
    val curTime = System.currentTimeMillis
    toRowReaders.map { case (partInfoOpt, rowReaders) =>
      partInfoOpt.map(_.toString).getOrElse("") + "\n\t" +
        rowReaders.take(partitionRowLimit).map {
          case reader =>
            val firstCol = if (formatTime && schema.isTimeSeries) {
              val timeStamp = reader.getLong(0)
              s"${new DateTime(timeStamp).toString()} (${(curTime - timeStamp)/1000}s ago)"
            } else {
              reader.getAny(0).toString
            }
            (firstCol +: (1 until schema.length).map(reader.getAny(_).toString)).mkString("\t")
        }.mkString("\n\t") + "\n"
    }
  }
}

/**
 * Converts various types to result types
 * TODO: consider collapsing into Result
 */
abstract class ResultMaker[A: TypeTag] {
  /**
   * Converts a source type like a Vector or Tuple to a result, with the given schema.
   * @param schema the schema of the result
   * @param chunkMethod used only for the VectorListResult to filter rows from the vectors
   * @param limit for Observables, limits the number of items to take
   */
  def toResult(input: A, schema: ResultSchema, chunkMethod: ChunkScanMethod, limit: Int): Task[Result]

  def fromResult(res: Result): A

  /**
   * Full type info including erased inner class info.  Needed to discern inner type of Observables.
   * Converted to a string and shortened to leave out the package namespaces
   */
  def typeInfo: String = {
    val typ = typeOf[A]
    s"${typ.typeSymbol.name}[${typ.typeArgs.map(_.typeSymbol.name).mkString(",")}]"
  }
}

object ResultMaker extends StrictLogging {
  implicit object UnitMaker extends ResultMaker[Unit] {
    // Unit should NEVER be the output of an ExecPlan.  Create an empty result if we ever desire that.
    def toResult(u: Unit,
                 schema: ResultSchema,
                 chunkMethod: ChunkScanMethod,
                 limit: Int = 1000): Task[Result] = ???
    def fromResult(res: Result): Unit = {}
  }
}

class ServiceUnavailableException(message: String) extends RuntimeException(message)
