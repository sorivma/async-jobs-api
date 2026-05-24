# Async Jobs API

REST API для запуска, просмотра и отмены фоновых задач. Хранение in-memory, выполнение через worker pool.

## Запуск

```bash
go run ./cmd/server
```

Сервис слушает `:8080`.

## Реализовано

- in-memory очередь задач;
- worker pool на goroutine;
- контекст отмены для каждой задачи;
- `POST /api/v1/jobs` возвращает `202 Accepted`;
- graceful shutdown с запретом новых задач;
- middleware: structured logging, request id, recoverer, timeout, shutdown guard, metrics;
- `/api/v1/metrics` в JSON-формате.

## Проверка

```bash
go test ./...
```
