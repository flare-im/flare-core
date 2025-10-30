#[derive(Debug, Clone)]
pub enum ControlCmd {
    Ping,
    Pong,
    AuthRequest(String, String, String), // user_id, platform, token
    AuthResponse(bool, String), // success, message
}

#[derive(Debug, Clone)]
pub struct DataCommand {
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CustomCommand {
    pub name: String,
    pub data: Vec<u8>,
    pub metadata: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum MessageCmd {
    Send(Vec<u8>),
    Ack { success: bool, status: i32, message_id: Option<String>, error_code: Option<u32>, error_message: Option<String> },
    Data(DataCommand),
    Custom(CustomCommand),
}

#[derive(Debug, Clone)]
pub enum NotificationCmd {
    System(String),
    Broadcast(String),
    Alert(String),
    Custom(CustomCommand),
}

#[derive(Debug, Clone)]
pub enum EventCmd {
    Open,
    Close,
    Reconnect,
    Custom(CustomCommand),
}

#[derive(Debug, Clone)]
pub enum Command {
    Control(ControlCmd),
    Message(MessageCmd),
    Notification(NotificationCmd),
    Event(EventCmd),
}

impl Command {
    pub fn command_type(&self) -> &'static str {
        match self {
            Command::Control(_) => "control",
            Command::Message(_) => "message",
            Command::Notification(_) => "notification",
            Command::Event(_) => "event",
        }
    }
}
