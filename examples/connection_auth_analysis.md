# 认证消息与连接消息的区别分析

## 1. 消息类型对比

### CONNECT/CONNECT_ACK
- **用途**: 建立网络连接
- **时序**: 连接建立阶段
- **数据内容**: 客户端ID、协议信息
- **层级**: 网络层面

### AUTH_REQUEST/AUTH_RESPONSE
- **用途**: 身份认证
- **时序**: 连接建立后
- **数据内容**: 用户ID、平台、令牌
- **层级**: 应用层面

## 2. 为什么不能合并

### 职责分离
```
CONNECT消息:
{
  "client_id": "client-123",
  "protocol": "auto"
}

AUTH_REQUEST消息:
{
  "user_id": "user-456",
  "platform": "web",
  "token": "token-xyz"
}
```

### 时序差异
1. 客户端 -> 服务端: CONNECT
2. 服务端 -> 客户端: CONNECT_ACK
3. 客户端 -> 服务端: AUTH_REQUEST
4. 服务端 -> 客户端: AUTH_RESPONSE

### 错误处理独立性
- 连接失败: 网络问题、协议不匹配
- 认证失败: 凭证错误、权限不足

## 3. 最佳实践

### 连接阶段
1. 先建立网络连接 (CONNECT/CONNECT_ACK)
2. 再进行身份认证 (AUTH_REQUEST/AUTH_RESPONSE)

### 平台信息传递
- 在CONNECT消息中通过metadata传递平台信息
- 在AUTH_REQUEST消息中通过payload传递平台信息

## 4. 代码示例

```rust
// 创建带平台信息的连接帧
let connect_frame = Frame::connect("client-123", Some("web"));

// 创建认证请求帧
let auth_frame = Frame::auth_request("user-456", "web", "token-xyz");
```

## 5. 总结

认证消息和连接消息有不同的职责，应该保持分离：
- **CONNECT**: 处理网络连接建立
- **AUTH_REQUEST**: 处理用户身份验证
- **平台信息**: 可以在两个消息中都携带，用于不同目的