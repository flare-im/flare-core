# 发布到 crates.io 指南

本文档说明如何将 `flare-core` 发布到 crates.io。

## 发布前检查清单

### 1. 代码质量检查

```bash
# 检查所有代码是否能编译通过
cargo check --all-targets

# 运行所有测试（如果有）
cargo test

# 检查代码格式
cargo fmt --check

# 运行 linter
cargo clippy --all-targets -- -D warnings
```

### 2. 文档检查

```bash
# 生成文档并检查
cargo doc --no-deps --open

# 检查文档中的所有链接
cargo doc --no-deps 2>&1 | grep -i "broken\|warning"
```

### 3. Cargo.toml 配置检查

确保 `Cargo.toml` 包含以下必需字段：

- ✅ `name` - 包名（必须是唯一的）
- ✅ `version` - 版本号（遵循语义化版本）
- ✅ `edition` - Rust 版本
- ✅ `description` - 简短描述（用于 crates.io 搜索）
- ✅ `license` - 许可证（或 `license-file`）
- ✅ `authors` - 作者列表
- ✅ `repository` - 仓库 URL（可选但推荐）
- ✅ `homepage` - 主页 URL（可选）
- ✅ `documentation` - 文档 URL（可选）
- ✅ `readme` - README 文件路径
- ✅ `keywords` - 关键词列表（用于搜索）
- ✅ `categories` - 分类列表（用于分类浏览）

### 4. 文件检查

确保以下文件存在且正确：

- ✅ `README.md` - 项目说明文档
- ✅ `LICENSE` - 许可证文件（如果使用 `license-file`）
- ✅ `src/lib.rs` - 库入口文件（必须存在）

### 5. 发布前测试

```bash
# 测试打包（不实际上传）
cargo package

# 检查打包后的文件列表
cargo package --list
```

## 发布步骤

### 1. 注册 crates.io 账号

如果还没有账号：

1. 访问 [crates.io](https://crates.io)
2. 使用 GitHub 账号登录
3. 获取 API Token：Settings → API Tokens → New Token
4. 保存 token（只会显示一次）

### 2. 配置本地 Cargo

```bash
# 登录 crates.io（需要输入 token）
cargo login <your-token>
```

### 3. 更新版本号

在 `Cargo.toml` 中更新版本号（遵循语义化版本）：

```toml
[package]
version = "0.1.0"  # 首次发布
```

### 4. 提交代码

```bash
# 确保所有更改已提交
git add .
git commit -m "Prepare for v0.1.0 release"
git tag v0.1.0
git push origin main
git push origin v0.1.0
```

### 5. 发布到 crates.io

```bash
# 发布（这会自动执行 cargo package 和上传）
cargo publish
```

### 6. 验证发布

发布后，访问以下链接验证：

- 包页面：`https://crates.io/crates/flare-core`
- 文档：`https://docs.rs/flare-core/0.1.0`

## 发布后操作

### 1. 创建 GitHub Release

在 GitHub 上创建 Release：

1. 访问仓库的 Releases 页面
2. 点击 "Draft a new release"
3. 选择版本标签（如 `v0.1.0`）
4. 填写 Release 说明
5. 发布

### 2. 更新文档

如果有文档网站，更新版本信息。

### 3. 更新 README

在 README 中添加 crates.io 和文档链接。

## 版本更新流程

### 补丁版本（0.1.0 → 0.1.1）

用于修复 bug，不破坏 API：

```toml
[package]
version = "0.1.1"
```

### 次要版本（0.1.0 → 0.2.0）

用于添加新功能，保持向后兼容：

```toml
[package]
version = "0.2.0"
```

### 主版本（0.1.0 → 1.0.0）

用于破坏性变更：

```toml
[package]
version = "1.0.0"
```

## 常见问题

### Q: 发布失败，提示 "package already exists"

A: 版本号已存在，需要更新版本号。

### Q: 发布失败，提示 "missing license"

A: 确保 `Cargo.toml` 中设置了 `license` 或 `license-file`。

### Q: 发布后文档未生成

A: 文档会在发布后几分钟内自动生成。如果长时间未生成，检查文档中的错误。

### Q: 如何更新已发布的版本？

A: 不能更新已发布的版本。只能发布新版本。

### Q: 如何撤销发布？

A: 不能完全撤销，但可以发布新版本修复问题，或在 README 中标记为废弃。

## 发布检查脚本

可以创建一个简单的检查脚本：

```bash
#!/bin/bash
set -e

echo "检查代码格式..."
cargo fmt --check

echo "运行 linter..."
cargo clippy --all-targets -- -D warnings

echo "检查文档..."
cargo doc --no-deps

echo "测试打包..."
cargo package

echo "所有检查通过！可以执行 cargo publish"
```

保存为 `check-release.sh`，使用 `chmod +x check-release.sh` 添加执行权限。

## 参考资料

- [Cargo 发布文档](https://doc.rust-lang.org/cargo/reference/publishing.html)
- [crates.io 发布指南](https://doc.rust-lang.org/cargo/reference/publishing.html)
- [语义化版本](https://semver.org/)

