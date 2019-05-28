package DataDog

case class PageViews(domain: String, page: String, views: Int = 0, response_size: Long = 0) {

  def getDomainPage: String = domain+page

}
