package au.org.healthdirect.nhsd.codingchallenge

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions.broadcast

import scala.util.Try

case class InputPayload(
  healthcareServices: Option[HealthcareService],
  organisations: Option[Organisation],
  locations: Option[Location],
  errors: Option[String]
)

object InputPayload {

  def healthcareService(hcs: HealthcareService): InputPayload =
    InputPayload(Some(hcs), None, None, None)

  def organisation(org: Organisation): InputPayload =
    InputPayload(None, Some(org), None, None)

  def location(loc: Location): InputPayload =
    InputPayload(None, None, Some(loc), None)

  def error(msg: String): InputPayload =
    InputPayload(None, None, None, Some(msg))
}

case class EventHandlerResult(
  pharmacies: Dataset[Pharmacy],
  gps: Dataset[GP],
  errors: Dataset[String]
)

object EventHandler {

  def processEvents(session: SparkSession, events: Dataset[RawEvent]): EventHandlerResult = {
    import session.implicits._

    val inputPayloads = loadPayloads(session, events)

    val result = processPayloads(
      session,
      inputPayloads.flatMap(_.healthcareServices),
      inputPayloads.flatMap(_.organisations),
      inputPayloads.flatMap(_.locations)
    )
    result.copy(errors = result.errors.union(inputPayloads.flatMap(_.errors.toList)))
  }

  def loadPayloads(session: SparkSession, events: Dataset[RawEvent]): Dataset[InputPayload] = {
    import session.implicits._

    events.map(event => {
      event.payload_type match {
        case "HealthcareService" => getHealthcareService(event)
        case "Organisation" => getOrganisation(event)
        case "Location" => getLocation(event)
        case _ => unknownPayloadType(event)
      }
    })
  }

  def createErrorMsg(event: RawEvent, msg: String): String =
    s"event_id: ${event.event_id} - $msg"


  def getHealthcareService(event: RawEvent): InputPayload = {
    val result = for {
      hcs <- parseHealthcareServicePayload(event.payload)
      _ <- validateName(hcs.name)
      _ <- validateServiceType(hcs.service_type)
    } yield hcs
    result.fold(msg => InputPayload.error(createErrorMsg(event, msg)), InputPayload.healthcareService)
  }

  def getOrganisation(event: RawEvent): InputPayload = {
    val result = for {
      org <- parseOrganisationPayload(event.payload)
      _ <- validateName(org.name)
    } yield org

    result.fold(msg => InputPayload.error(createErrorMsg(event, msg)), InputPayload.organisation)
  }

  def getLocation(event: RawEvent): InputPayload = {
    val result = for {
      loc <- parseLocationPayload(event.payload)
      _ <- validateAddress(loc.address)
    } yield loc

    result.fold(msg => InputPayload.error(createErrorMsg(event, msg)), InputPayload.location)
  }

  def unknownPayloadType(event: RawEvent): InputPayload =
    InputPayload.error(createErrorMsg(event, s"Unknown payload_type ${event.payload_type}"))

  // hint: Spark uses json4s-jackson so its already on the class path, but you can use any json parser
  def parseHealthcareServicePayload(payload: String): OperationResult[HealthcareService] =
    parsePayload[HealthcareService](payload)

  def parseOrganisationPayload(payload: String): OperationResult[Organisation] =
    parsePayload[Organisation](payload)

  def parseLocationPayload(payload: String): OperationResult[Location] =
    parsePayload[Location](payload)

  def parsePayload[T: Manifest](payload: String): OperationResult[T] = {
    implicit val formats = DefaultFormats
    val parsed = for {
      parsed <- Try(parse(payload))
      extracted <- Try(parsed.extract[T])
    } yield extracted

    parsed match {
      case scala.util.Success(value) => OperationResult.success(value)
      case scala.util.Failure(exception) => OperationResult.failure(exception.getMessage)
    }
  }

  // Name can not be empty
  def validateName(name: String): OperationResult[Unit] =
    nonEmptyString(name, "Missing name for Organisation payload")

  // Type must be either Pharmacy or GP
  def validateServiceType(serviceType: String): OperationResult[Unit] =
    serviceType match {
      case "Pharmacy" => OperationResult.success(())
      case "GP" => OperationResult.success(())
      case invalid => OperationResult.failure(s"Invalid serviceType: '$invalid'")
    }

  // Address must have non empty street/suburb/state and postcode must be a 4 digit number
  def validateAddress(addr: Address): OperationResult[Unit] =
    for {
      _ <- nonEmptyString(addr.street, "Missing street for address")
      _ <- nonEmptyString(addr.suburb, "Missing suburb for address")
      _ <- nonEmptyString(addr.state, "Missing state for address")
      _ <- nonEmptyString(addr.postcode, "Missing postcode for address")
      _ <- validate(
        addr.postcode.length == 4 && addr.postcode.forall(_.isDigit),
        "Postcode should be 4 digit number"
      )
    } yield ()

  def nonEmptyString(s: String, errorMsg: String): OperationResult[Unit] =
    validate(!StringUtils.isBlank(s), errorMsg)

  def validate[T](f: => Boolean, errorMsg: String): OperationResult[Unit] =
    if (f)
      OperationResult.success(())
     else
      OperationResult.failure(errorMsg)


  /*
   1. Assumes that all pharmacies and GPs join with the location and organisations.
      If they don't it is assumed to be an error condition

   2. Assume location and org data is small and can be broad casted
   */
  def processPayloads(
    session: SparkSession,
    healthcareServices: Dataset[HealthcareService],
    organisations: Dataset[Organisation],
    locations: Dataset[Location]
  ): EventHandlerResult = {
    import DataSetUtils._
    import session.implicits._

    val gpLocations = healthcareServices
      .filter(_.service_type == "GP")
      .joinWith(broadcast(locations), healthcareServices("loc_id") === locations("id"), "left")
      .cache()

    val gpsMissingLoc = gpLocations.collectP{ case (h, null) =>
      s"Address Id: '${h.loc_id}' cannot be found for Health care Service with id: '${h.id}'"
    }
    val gps =  gpLocations.collectP{
      case (h, l) if h != null && l != null =>
        GP(h.id, h.name, l.address)
    }

    val pharmaciesLocOrg = healthcareServices
      .filter(_.service_type == "Pharmacy")
      .joinWith(broadcast(locations), healthcareServices("loc_id") === locations("id"), "left")
      .joinWith(broadcast(organisations), $"_1.org_id" === organisations("id"), "left")
      .cache()

    val pharmaciesMissingLoc = pharmaciesLocOrg.collectP{ case ((p, null), _) =>
      s"Address Id: '${p.loc_id}' cannot be found for Health care Service with Id: '${p.id}'"
    }

    val pharmaciesMissingOrg = pharmaciesLocOrg.collectP{ case ((p, _), null) =>
      s"Organisation Id: '${p.org_id}' cannot be found for Health care Service with Id : '${p.id}'"
    }

    val errors = gpsMissingLoc.union(pharmaciesMissingLoc).union(pharmaciesMissingOrg)

    val pharmacies = pharmaciesLocOrg
      .collectP {
        case ((h, l), o) if h != null && l != null && o != null =>
          Pharmacy(h.id, h.name, l.address, o.name)
      }

    EventHandlerResult(pharmacies, gps, errors)
  }
}
