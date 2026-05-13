# Test server runbook

Короткая инструкция для тестового контура на общем сервере.

Важно: сервер один для production и test. Production-процессы работают из `/root/inventory`, тестовый проект лежит в `/opt/test_project`. Не запускать тяжёлые scheduler-сервисы в тесте без необходимости и не оставлять dev-процессы висеть после проверки.

## Подключение

```bash
ssh root@164.92.213.254
```

Перейти в тестовый проект:

```bash
cd /opt/test_project
```

Проверить, где находишься:

```bash
pwd
git branch --show-current
git status
```

## Обновление кода

```bash
cd /opt/test_project
git pull origin main
```

Если нужна другая ветка, сначала явно переключить её по обычному git workflow проекта.

## Backend

Команды запуска такие же, как локально.

Подготовить окружение:

```bash
cd /opt/test_project
source venv/bin/activate
```

Проверить миграции:

```bash
alembic current
alembic heads
alembic upgrade head
```

Запустить тестовый backend на `8001` в фоне:

```bash
cd /opt/test_project
source venv/bin/activate
nohup uvicorn app.main:app --host 0.0.0.0 --port 8001 > backend-test.log 2>&1 &
```

Проверить:

```bash
ss -ltnp | grep ':8001'
curl -I http://127.0.0.1:8001/docs
tail -f /opt/test_project/backend-test.log
```

Остановить тестовый backend:

```bash
pkill -f "/opt/test_project/venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8001"
```

## Frontend

```bash
cd /opt/test_project/admin-panel
npm install
npm run build
```

Запустить тестовый frontend dev server на `3001` в фоне:

```bash
cd /opt/test_project/admin-panel
nohup env HOST=0.0.0.0 PORT=3001 BROWSER=none WDS_SOCKET_PORT=3001 npm start > frontend-test.log 2>&1 &
```

Открыть в браузере:

```text
http://164.92.213.254:3001
```

Проверить:

```bash
ss -ltnp | grep ':3001'
curl -I http://127.0.0.1:3001
tail -f /opt/test_project/admin-panel/frontend-test.log
```

Остановить тестовый frontend:

```bash
pkill -f "/opt/test_project/admin-panel/node_modules/react-scripts/scripts/start.js"
pkill -f "react-scripts start"
```

## Проверка нагрузки

Перед запуском и после теста смотреть память, swap и тяжёлые процессы:

```bash
free -h
ps -eo pid,ppid,%mem,%cpu,stat,etime,cmd --sort=-%mem | head -30
```

Проверить тестовые процессы:

```bash
ps -eo pid,ppid,stat,etime,cmd | grep -E "/opt/test_project|--port 8001|react-scripts|npm start" | grep -v grep
```

Проверить production-процессы, чтобы случайно их не трогать:

```bash
ps -eo pid,ppid,stat,etime,cmd | grep -E "/root/inventory|--port 8000" | grep -v grep
```

## Правило для общего хоста

Для теста запускать только то, что нужно прямо сейчас:

- backend на `8001`;
- frontend на `3001`;
- без scheduler-сервисов, если тест не требует их явно;
- после проверки остановить тестовые процессы.

Production на `/root/inventory` и порту `8000` не трогать при работе с тестом.
