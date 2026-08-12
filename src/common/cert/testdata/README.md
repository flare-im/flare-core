# 测试固件

这里的 `.pem` 是**一次性的、只为单元测试而生成的**密钥与自签证书，
不对应任何真实身份，也没有在任何环境里被使用过或部署过。

它们存在的意义是：`converter.rs` 的 PEM 解析必须对**真实编码**做断言。
用手工拼的假 PEM 测不出编码层面的回归——而这个文件恰恰因为换过解析库
（rustls-pemfile → rustls-pki-types）才格外需要真样本。

生成方式（可随时重新生成，无需保密）：

```bash
openssl req -x509 -newkey ed25519 -keyout k8.pem -out cert.pem -days 3650 -nodes -subj "/CN=flare-test"
openssl ecparam -genkey -name prime256v1 -noout -out sec1.pem
openssl genrsa -traditional -out pkcs1.pem 2048
```

| 文件 | 内容 | 用途 |
| --- | --- | --- |
| `cert.pem` | 自签 ed25519 证书 | 证书解析 |
| `k8.pem` | PKCS#8 私钥 | 应被接受 |
| `sec1.pem` | SEC1（EC prime256v1）私钥 | 应被接受 |
| `pkcs1.pem` | PKCS#1（RSA traditional）私钥 | **应被拒绝**，锁住「换库不得扩大接受范围」 |

密钥扫描工具可能会把这个目录标成疑似泄漏——这份说明就是为此存在的。
