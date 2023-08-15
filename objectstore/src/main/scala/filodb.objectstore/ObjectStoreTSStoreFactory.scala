package filodb.objectstore;

import com.typesafe.config.Config
import monix.execution.Scheduler

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.FiloSessionProvider
import filodb.coordinator.StoreFactory
import filodb.core.memstore.TimeSeriesMemStore
import filodb.objectstore.columnstore.ObjectStoreColumnStore
import filodb.objectstore.metastore.ObjectStoreMetaStore


class ObjectStoreTSStoreFactory(config: Config, ioPool: Scheduler) extends StoreFactory {
    val cassandraConfig = config.getConfig("cassandra")
    val objectStoreonfig = config.getConfig("objectstore")
    val session = FiloSessionProvider.openSession(cassandraConfig)
    val colStore = new ObjectStoreColumnStore(config, ioPool)(ioPool)
    val metaStore = new ObjectStoreMetaStore(objectStoreonfig)(ioPool)
    val memStore = new TimeSeriesMemStore(config, colStore, metaStore)(ioPool)
}
