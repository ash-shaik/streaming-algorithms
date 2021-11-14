package model 

import java.time.LocalDate

case class Clickstream(user_id: Int,
                       device: String,
                       client_event: String,
                       client_timestamp: LocalDate)
