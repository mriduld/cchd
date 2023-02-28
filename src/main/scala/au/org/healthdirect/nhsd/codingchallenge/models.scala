package au.org.healthdirect.nhsd.codingchallenge

case class RawEvent(event_id: String, event_time: String, payload_type: String, payload: String)

case class HealthcareService(id: String, name: String, service_type: String, org_id: String, loc_id: String)

case class Organisation(id: String, name: String)

case class Location(id: String, address: Address)

case class Pharmacy(id: String, name: String, address: Address, owner_name: String)

case class GP(id: String, name: String, address: Address)

case class Address(street: String, suburb: String, state: String, postcode: String)
