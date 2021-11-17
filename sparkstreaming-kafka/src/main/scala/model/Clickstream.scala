package model 

import java.sql.Timestamp

case class Clickstream(user_id: Int,
                       device: String,
                       client_event: String,
                       client_timestamp: Timestamp)
