package DataDog

case class DomainPage(domain: String = "", page: String = "") {

  def getName: String = domain + page

  def belongsTo(that: DomainPage): Boolean ={
    (this.domain == that.domain) && (this.page == that.page || that.page.isEmpty)
  }

  override def equals(obj: Any): Boolean = obj match {
//    case that: DomainPage => this.belongsTo(that) || that.belongsTo(this)
    case that: DomainPage => this.hashCode() == that.hashCode() || this.belongsTo(that)
    case _ => super.equals(obj)
  }

  override def hashCode(): Int = (domain+page).hashCode
}

case class PageViews(domain: String = "", page: String = "", views: Int = 0, response_size: Long = 0) {

  val domainPage = new DomainPage(domain, page)

  def getDomainPageName: String = domain+page

  def getDomainPage: DomainPage = this.domainPage

  override def hashCode(): Int = domainPage.hashCode

}

object PageViews{

  def apply(line: String): PageViews = {
    val info = line.split(" ")
    val domain = if(info.isEmpty) "" else info(0)
    val page = if(info.length <= 1) "" else info(1)
    val views = if(info.length <= 2) 0 else info(2).toInt
    val response_size = if(info.length <= 3) 0L else info(3).toLong
    new PageViews(domain, page, views, response_size)
  }

}
