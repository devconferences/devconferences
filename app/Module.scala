import com.google.inject.AbstractModule

class Module extends AbstractModule {

  override def configure() = {
    bind(classOf[DataLoader])
      .to(classOf[ElasticsearchDataLoader])
      .asEagerSingleton
  }
}