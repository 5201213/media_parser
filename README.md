# 微信机器人媒体解析插件

适用于 [chatgpt-on-wechat](https://github.com/zhayujie/chatgpt-on-wechat) 项目的媒体解析插件。

## 功能
- 支持解析多平台短视频无水印链接
- 支持解析多平台图集
- 自动去除水印
- 支持缓存管理
- 多线程下载

## 支持平台
### 视频平台
- 抖音
- 快手
- 小红书
- 皮皮虾
- 西瓜视频
- 最右
- 火山
- 微博
- 微视
- 绿洲
- Bilibili
- 陌陌
- 全民视频
- 全民K歌
- 逗拍
- 美拍
- 六间房视频
- 梨视频
- 虎牙
- 新片场
- AcFun

### 图集平台
- 抖音
- 快手
- 小红书
- 皮皮虾
- 西瓜
- 最右

## 安装方法
1. 进入 chatgpt-on-wechat/plugins 目录
2. 克隆本项目: `git clone https://github.com/5201213/media-parser`
3. 安装依赖: `pip install Pillow requests`

## 配置
插件支持通过 `config.json` 文件进行配置：
- `api_endpoints`: API 地址配置
- `supported_platforms`: 支持的平台列表
- `cache`: 缓存管理设置
- `download`: 下载参数配置

## 使用说明
1. 直接发送视频或图集链接
2. 机器人将自动解析并发送无水印内容
3. 支持多图和视频解析

## 注意事项
- 解析依赖第三方 API，可能受限于 API 的稳定性
- 建议定期更新插件以获得最佳体验

## 贡献
欢迎提交 Issues 和 Pull Requests！

## 许可证
[MIT License](LICENSE)
