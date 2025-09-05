//! QUIC Protobuf测试脚本
//! 
//! 自动化测试QUIC连接中使用Protobuf序列化的通信功能

fn main() {
    println!("🚀 启动QUIC Protobuf通信测试");
    println!("请参考项目文档运行对应的测试脚本");
    println!("");
    println!("测试说明：");
    println!("  1. 首先运行QUIC Protobuf回显服务端");
    println!("  2. 然后运行QUIC Protobuf回显客户端");
    println!("  3. 客户端将发送Protobuf序列化的消息到服务端");
    println!("  4. 服务端将回显这些消息");
    println!("  5. 客户端接收回显消息并验证通信");
    println!("");
    println!("编译测试示例：");
    println!("  编译服务端示例：cargo build --example quic_protobuf_echo_server");
    println!("  编译客户端示例：cargo build --example quic_protobuf_echo_client");
    println!("");
    println!("运行测试：");
    println!("  启动服务端：cargo run --example quic_protobuf_echo_server");
    println!("  在另一个终端启动客户端：cargo run --example quic_protobuf_echo_client");
}