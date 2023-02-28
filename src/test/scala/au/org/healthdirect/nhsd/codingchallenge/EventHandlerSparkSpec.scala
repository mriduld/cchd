package au.org.healthdirect.nhsd.codingchallenge

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EventHandlerSparkSpec extends FunSuite with DatasetComparer {
  test("Returns invalid json as error") {
    val events = List(
      RawEvent("e1", "2022-01-01T00:00:00.000Z", "HealthcareService", "Invalid Json")
    )
    SparkTestUtils.withSpark { session =>
      import session.implicits._
      val eventDS = events.toDS()
      val result = EventHandler.processEvents(session, eventDS)
      assertSmallDatasetEquality(result.pharmacies, session.emptyDataset[Pharmacy])
      assertSmallDatasetEquality(result.gps, session.emptyDataset[GP])
      assert(result.errors.count() > 0)
    }
  }

  test("Validation errors") {
    val events = List(
      RawEvent("e1", "2022-01-01T00:00:00.000Z", "HealthcareService",
        """{"id": "1", "name": "", "service_type": "Pharmacy", "org_id": "o1", "loc_id": "l1"}""" ),
      RawEvent("e2", "2022-01-01T00:00:00.000Z", "HealthcareService",
        """{"id": "2", "name": "GP", "service_type": "Hospital", "org_id": "o1", "loc_id": "l1"}""" ),
      RawEvent("e3", "2022-01-01T00:00:00.000Z", "HealthcareService",
        """{"id": "3", "name": "Pharmacy", "service_type": "Pharmacy", "org_id": "o1", "loc_id": "l1"}""" ),
      RawEvent("e4", "2022-01-01T00:00:00.000Z", "HealthcareService",
        """{"id": "4", "name": "GP", "service_type": "Hospital", "org_id": "o1", "loc_id": "l1"}""" ),
      RawEvent("e5", "2022-01-01T00:00:00.000Z", "Organisation",
        """{"id": "o1", "name": null }""" ),
      RawEvent("e6", "2022-01-01T00:00:00.000Z", "Location",
        """{"id": "l1", "address": {"street": "", "suburb": "Melbourne", "state": "VIC", "postcode": "3000" } }""" ),
      RawEvent("e7", "2022-01-01T00:00:00.000Z", "Location",
        """{"id": "l1", "address": {"street": "test", "suburb": null, "state": "VIC", "postcode": "3000" } }""" ),
      RawEvent("e8", "2022-01-01T00:00:00.000Z", "Location",
        """{"id": "l1", "address": {"street": "test", "suburb": "Melbourne", "state": "", "postcode": "3000" } }""" ),
      RawEvent("e9", "2022-01-01T00:00:00.000Z", "Location",
        """{"id": "l1", "address": {"street": "test", "suburb": "Melbourne", "state": "VIC", "postcode": "300" } }""" ),
      RawEvent("e10", "2022-01-01T00:00:00.000Z", "Location",
        """{"id": "l1", "address": {"street": "test", "suburb": "Melbourne", "state": "VIC", "postcode": "abcd" }}""" ),
      RawEvent("e11", "2022-01-01T00:00:00.000Z", "HealthcareService",
        """{"id": "5", "name": "Pharmacy", "service_type": "Pharmacy", "org_id": null, "loc_id": null}""" ),
    )
    SparkTestUtils.withSpark { session =>
      import session.implicits._
      val eventDS = events.toDS()
      val result = EventHandler.processEvents(session, eventDS)
      val expectedErrors = List(
        "Address Id: 'l1' cannot be found for Health care Service with Id: '3'",
        "Organisation Id: 'o1' cannot be found for Health care Service with Id : '3'",
        "Address Id: 'null' cannot be found for Health care Service with Id: '5'",
        "Organisation Id: 'null' cannot be found for Health care Service with Id : '5'",
        "event_id: e1 - Missing name for Organisation payload",
        "event_id: e2 - Invalid serviceType: 'Hospital'",
        "event_id: e4 - Invalid serviceType: 'Hospital'",
        "event_id: e5 - Missing name for Organisation payload",
        "event_id: e6 - Missing street for address",
        "event_id: e7 - Missing suburb for address",
        "event_id: e8 - Missing state for address",
        "event_id: e9 - Postcode should be 4 digit number",
        "event_id: e10 - Postcode should be 4 digit number"
      )
      assertSmallDatasetEquality(result.errors, expectedErrors.toDS(), orderedComparison = false)
      assertSmallDatasetEquality(result.pharmacies, session.emptyDataset[Pharmacy], orderedComparison = false)
      assertSmallDatasetEquality(result.gps, session.emptyDataset[GP])

    }
  }

  test("Returns no errors") {
    val events = List(
      RawEvent("e1", "2022-01-01T00:00:00.000Z", "HealthcareService",
        """{"id": "1", "name": "Pharmacy", "service_type": "Pharmacy", "org_id": "o1", "loc_id": "l1"}""" ),
      RawEvent("e2", "2022-01-01T00:00:00.000Z", "HealthcareService",
        """{"id": "2", "name": "GP", "service_type": "GP", "org_id": "o1", "loc_id": "l1"}""" ),
      RawEvent("e3", "2022-01-01T00:00:00.000Z", "Location",
        """{"id": "l1", "address": {"street": "1 Avenue", "suburb": "Melbourne", "state": "VIC", "postcode": "3000" } }""" ),
      RawEvent("e4", "2022-01-01T00:00:00.000Z", "Organisation",
        """{"id": "o1", "name": "Some org" }""" )
    )
    SparkTestUtils.withSpark { session =>
      import session.implicits._
      val eventDS = events.toDS()
      val result = EventHandler.processEvents(session, eventDS)
      val address = Address("1 Avenue", "Melbourne", "VIC", "3000")
      val expectedPharmacyDS = List(Pharmacy("1", "Pharmacy", address, "Some org")).toDS()
      val expectedGPDS = List(GP("2", "GP", address)).toDS()
      assertSmallDatasetEquality(result.pharmacies, expectedPharmacyDS)
      assertSmallDatasetEquality(result.gps, expectedGPDS)
      assertSmallDatasetEquality(result.errors, session.emptyDataset[String])
    }
  }
}
