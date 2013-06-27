//simple curl
import scalaj.http.Http

val result = Http.postData("http://example.com/url", """{"id":"12","json":"data"}""")
    .header("Content-Type", "application/json")
    .header("Charset", "UTF-8")
    .option(HttpOptions.readTimeout(10000))
    .responseCode