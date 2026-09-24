# Entity Alpha Engine

Один Python-файл: сбор данных из API Hub, реконструкция действий entity, двойная LLM-классификация через grok-4.5/grok-4.6, исторические market features, строгий train/validation/test backtest, multiple-testing control и Streamlit dashboard.

## 1. ENV

В `.env` рядом со скриптом:

```env
API_HUB_KEY=...
LLMASS_API_KEY=...
LLMASS_BASE_URL=https://llmass.arbitron.dev/v1
API_HUB_BASE_URL=https://hub.arbitron.dev
```

`LLMASS_API_KEY` уже можно оставить тем, который у тебя есть. Ключи никогда не кладутся в код или БД.

## 2. Установка

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

## 3. Проверка API и автоматическое определение нужных маршрутов Hub

```bash
python entity_alpha.py doctor
```

Скрипт читает live `openapi.json`, сам находит подходящие Arkham / DeBank / market / RPC routes и кэширует выбранные маршруты в SQLite. Это сделано специально, чтобы проект не ломался при переименовании конкретных endpoint'ов Hub.

## 4. Полный прогон

Быстрый нормальный старт:

```bash
python entity_alpha.py all --days 730
```

Можно задать свои entity:

```bash
python entity_alpha.py all --days 730 --entities "Wintermute,Jump Trading,Cumberland,Galaxy Digital,Amber Group,GSR,DWF Labs"
```

Этапы по отдельности:

```bash
python entity_alpha.py collect --days 730
python entity_alpha.py classify
python entity_alpha.py enrich
python entity_alpha.py backtest
```

## 5. Dashboard

```bash
streamlit run entity_alpha.py
```

В dashboard есть:

- Overview: объём данных и покрытие;
- Events: каждое реконструированное действие, цепочка транзакций и оба LLM-голоса;
- Entity study: forward returns конкретной entity/action/token;
- Pattern scanner: найденные закономерности только после train/validation gate;
- Backtest: test equity curve, hit rate, profit factor, max drawdown, bootstrap CI, permutation p-value, FDR q-value;
- Data quality: какие возможности Hub доступны/недоступны и где данных не хватило.

## Принцип защиты от самообмана

- событие всегда строится только из данных, известных к моменту события;
- вход в backtest — после `EXECUTION_DELAY_MIN`, а не по цене до сигнала;
- направление, горизонт и thresholds выбираются на TRAIN;
- гипотеза проверяется на VALIDATION;
- multiple testing корректируется Benjamini-Hochberg;
- TEST никогда не используется для выбора паттерна, только для финального отчёта;
- current portfolio/liquidity не подставляются в историческое событие: если исторического значения нет, feature остаётся NULL;
- LLM классифицирует смысл транзакционной последовательности, но не решает, покупать или продавать.

## Что именно классифицирует LLM

`CEX_DEPOSIT`, `CEX_WITHDRAWAL`, `DEX_BUY`, `DEX_SELL`, `LP_ADD`, `LP_REMOVE`, `BRIDGE_IN`, `BRIDGE_OUT`, `BORROW`, `REPAY`, `LEND`, `STAKE`, `UNSTAKE`, `OTC_OR_INTERNAL`, `TRANSFER`, `UNKNOWN`.

grok-4.5 и grok-4.6 голосуют независимо. При несогласии grok-4.6 делает отдельный judge-pass. Низкоуверенные события сохраняются, но исключаются из scanner по умолчанию.

## Важное ограничение

API Hub — read/data layer. Этот проект не подписывает транзакции и не торгует реальными деньгами. Он строит исследовательскую базу и backtest. Исполнение стоит добавлять только после устойчивого out-of-sample результата.
