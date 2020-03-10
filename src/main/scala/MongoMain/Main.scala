package MongoMain

import org.mongodb.scala._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.util.{Failure, Success}
import MongoMain.Helpers._
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Projections._

object Main extends App {
  val mongoClient: MongoClient = MongoClient()
  val database: MongoDatabase = mongoClient.getDatabase("mydb")
  val collection: MongoCollection[Document] = database.getCollection("test")

  val doc: Document = Document("name" -> "MongoDB", "type" -> "database",
    "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))

  collection.deleteMany(gte("i", 1)).printHeadResult("Delete Result: ")
  collection.deleteMany(gte("name", "MongoDB")).printHeadResult("Delete Result: ")

  collection.insertOne(doc).results()

  val documents = (1 to 100) map { i: Int => Document("i" -> i) }

  val insertObservable = collection.insertMany(documents)

  val insertAndCount = for {
    insertResult <- insertObservable
    countResult <- collection.countDocuments()
  } yield countResult

  val futureResult = insertAndCount.headOption()

  futureResult.onComplete {
    case Success(value) => println(s"total # of documents after inserting 100 small ones (should be 101):  $value")
    case Failure(exception) => exception.printStackTrace()
  }

  Thread.sleep(2000)

  collection.find().printResults()

  collection.find(and(gt("i", 50), lte("i", 100))).printResults()

  collection.find(exists("i")).sort(descending("i")).first().printHeadResult()

  collection.find().projection(excludeId()).first().printHeadResult()

  collection.aggregate(Seq(filter(gt("i", 0)),
    project(Document("""{ITimes10: {$multiply: ["$i", 10]}}""")))
  ).printResults()

  collection.aggregate(List(group(null, sum("total", "$i")))).printHeadResult()
}
