// OPCUA for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2017-2024 Adam Lock

//! This simple OPC UA client will do the following:
//!
//! 1. Create a client configuration
//! 2. Connect to an endpoint specified by the url with security None
//! 3. Subscribe to values and loop forever printing out their valrap();
use std::{sync::Arc, vec};
use std::fmt::{write, Write};
use std::time::Duration;
use dotenvy::{dotenv_override, var};
use opcua::client::prelude::*;
use opcua::sync::*;

use kafka::producer::{Producer, Record, RequiredAcks};

fn main() {
    dotenv_override().ok();
    let opcua_host: &str = &var("OPCUA_SERVER").unwrap();
    let monitored_tags  = var("MONITORED_TAGS").unwrap();
    println!("OPC UA tags: {:?}", monitored_tags);
    let mut client = ClientBuilder::new()
        .application_name("DCS OPC UA client")
        .application_uri("urn:DCSOPCUAClient")
        .create_sample_keypair(true)
        .trust_server_certs(true)
        .session_retry_limit(3)
        .client().unwrap();

    // Create an endpoint. The EndpointDescription can be made from a tuple consisting of
    // the endpoint url, security policy, message security mode and user token policy.
    let endpoint: EndpointDescription = (opcua_host, "None", MessageSecurityMode::None, UserTokenPolicy::anonymous()).into();

    // Create the session
    let session = client.connect_to_endpoint(endpoint, IdentityToken::Anonymous).unwrap();

    // Create a subscription and monitored items
    if subscribe_to_values(session.clone(), monitored_tags).is_ok() {
        Session::run(session);
    } else {
        println!("Error creating subscription");
    }
}

fn subscribe_to_values(session: Arc<RwLock<Session>>, monitored_tags: String) -> Result<(), StatusCode> {
    let session = session.write();
    // Create a subscription polling every 2s with a callback
    let subscription_id = session.create_subscription(0.0, 3, 0, 0, 0, true, DataChangeCallback::new(|changed_monitored_items| {
        println!("Data change from server:");
        changed_monitored_items.iter().for_each(|item| print_value(item));
    }))?;
    // Create some monitored items   
    let monitored_tags_list: Vec<String> = monitored_tags.split(',').map(|tags|tags.trim().to_string()).collect();
    println!("Monitored tags: {:?}", monitored_tags_list);
    let items_to_create: Vec<MonitoredItemCreateRequest> = monitored_tags_list.iter()
        .map(|v| NodeId::new(2, v.clone()).into()).collect();
    let _ = session.create_monitored_items(subscription_id, TimestampsToReturn::Both, &items_to_create)?;
    Ok(())
}

fn print_value(item: &MonitoredItem) {
   let node_id = &item.item_to_monitor().node_id;
   let data_value = item.last_value();
   if let Some(ref value) = data_value.value {
       println!("Item \"{}\", Value = {:?}", node_id, value);
   } else {
       println!("Item \"{}\", Value not found, error: {}", node_id, data_value.status.as_ref().unwrap());
   }
}

#[cfg(test)]
mod test {
    use super::*;
}
pub fn send_kafka(broker: String, item: &MonitoredItem) {
    let mut producer = 
        Producer::from_hosts(vec!(broker.to_owned()))
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();
    let mut buf = String::with_capacity(2);
    let _ = write!(&mut buf, "{:?}", item.last_value().value);
    producer.send(&Record { key: ("start_kanban"), value: (buf.as_bytes()), topic: ("pss"), partition: (1) }).unwrap();
    buf.clear();
}
// pub async fn send_to_kafka(broker: &str, topic: &str, key: &str, payload: &str) -> Result<(), Box<dyn std::error::Error>> {
//     let producer: Producer = ClientConfig::new()
//         .set("bootstrap.servers", broker)
//         .set("message.timeout.ms", "5000")
//         .create()?;

//     producer
//         .send(
//             FutureRecord::to(topic)
//             .key(key)
//             .payload(payload), 
//             Duration::from_secs(0),
//         )
//         .await
//         .map_err(|(e, _)| e)?;
//         println!("Message sent to topic: {}", topic);
//         Ok(())
// }