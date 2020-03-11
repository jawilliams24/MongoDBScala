package MongoMain

import org.mongodb.scala._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.util.{Failure, Success}
import MongoMain.Helpers._

import scala.util.control.Breaks
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.{BulkWriteOptions, DeleteOneModel, InsertOneModel, ReplaceOneModel, UpdateOneModel, WriteModel}


object Main extends App {
  val mongoClient: MongoClient = MongoClient()
  val database: MongoDatabase = mongoClient.getDatabase("mydb")
  val collection: MongoCollection[Document] = database.getCollection("test")
  val loop = new Breaks
  val doc: Document = Document("name" -> "MongoDB", "type" -> "database",
    "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))

  var connected = false
  while (!connected) {
    Thread.sleep(1000)
    val userChoice = scala.io.StdIn.readLine("Please select an option from: create, create 100, read, update, delete, delete all, exit > ")
    loop.breakable {

      userChoice match {

        case ("create") =>
          collection.insertOne(doc).results()
          loop.break()

        case ("create 100") =>
          val documents = (1 to 100) map { i: Int => Document("i" -> i) }
          val insertObservable = collection.insertMany(documents)
          val insertAndCount = for {
            insertResult <- insertObservable
            countResult <- collection.countDocuments()
          } yield countResult
          loop.break()

        case ("read") =>
          collection.find().printResults()
          loop.break()

        case ("update") =>
          collection.updateOne(equal("i", 10),
            set("i", 110)).printHeadResult("Update Result: ")
          loop.break()

        case ("delete") =>
          collection.deleteOne(equal("_id",
            scala.io.StdIn.readLine("Please enter the id you wish to delete: ").toInt))
            .printHeadResult("Delete Result: ")
          loop.break()

        case ("delete all") =>
          collection.deleteMany(gte("i", 1))
            .printHeadResult("Delete Result: ")
          collection.deleteMany(gte("_id", 1))
            .printHeadResult("Delete Result: ")
          collection.deleteMany(gte("name", "MongoDB"))
            .printHeadResult("Delete Result: ")
          loop.break()

        case ("stop") =>
          connected = true

      }
    }


  }

  //
  //
  //  val documents = (1 to 100) map { i: Int => Document("i" -> i) }
  //
  //  val insertObservable = collection.insertMany(documents)
  //
  //
  //  val futureResult = insertAndCount.headOption()
  //
  //  futureResult.onComplete {
  //    case Success(value) => println(s"total # of documents after inserting 100 small ones (should be 101):  $value")
  //    case Failure(exception) => exception.printStackTrace()
  //  }
  //
  //  Thread.sleep(2000)
  //
  //
  //  collection.find(and(gt("i", 50), lte("i", 100))).printResults()
  //
  //  collection.find(exists("i")).sort(descending("i")).first().printHeadResult()
  //
  //  collection.find().projection(excludeId()).first().printHeadResult()
  //
  //  collection.aggregate(Seq(filter(gt("i", 0)),
  //    project(Document("""{ITimes10: {$multiply: ["$i", 10]}}""")))
  //  ).printResults()
  //
  //  collection.aggregate(List(group(null, sum("total", "$i")))).printHeadResult()
  //
  //  collection.find(equal("i", 71)).first().printHeadResult()
  //
  //  collection.find(gt("i", 50)).printResults()
  //
  //  collection.updateMany(lt("i", 100), inc("i", 100)).printHeadResult("Update Result: ")
  //
  //  val writes: List[WriteModel[_ <: Document]] = List(
  //      InsertOneModel(Document("_id" -> 4)),
  //      InsertOneModel(Document("_id"-> 5)),
  //      InsertOneModel(Document("_id" -> 6)),
  //      UpdateOneModel(Document("_id" -> 1), set("x", 2)),
  //      DeleteOneModel(Document("_id" -> 2)),
  //      ReplaceOneModel(Document("_id" -> 3), Document("_id" -> 3, "x" -> 4))
  //  )
  //
  //  collection.bulkWrite(writes).printHeadResult("Bulk write results: ")
  //
  //  collection.bulkWrite(writes, BulkWriteOptions().ordered(false)).printHeadResult("Bulk write results (unordered): ")


}
