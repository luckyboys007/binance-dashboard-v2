# Binance Dashboard V2

实时监控 Binance USDT 合约大单资金流，含信号系统、资金费率、聪明钱指标。

## 运行

```bash
pip install fastapi uvicorn
python app.py
# 访问 http://localhost:18999
```

## 功能

- **信号系统**：顺势做多/做空、大单护盘/砸盘、抄底、机构/鲸鱼信号、极端波动、净流入流出
- **资金费率**：实时拉取 Binance fapi 资金费率 Top 20
- **聪明钱指标**：基于大单买卖力量比计算
- **多周期**：5分钟、1小时、24小时、72小时、7天净流入

## 依赖

- Python 3.11+
- fastapi
- uvicorn
- aiosqlite
