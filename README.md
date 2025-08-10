# otus-antifraud

Учебный репозиторий для курса Otus MLOps. Домашнее задание №1 — предварительный анализ задачи антифрода и планирование проекта.

## Документация

- [Цели и метрики](docs/01_goals_and_metrics.md)
- [MISSION Canvas](docs/02_mission_canvas.md)
- [Декомпозиция системы](docs/03_system_decomposition.md)
- [S.M.A.R.T. задачи (MVP)](docs/04_smart_tasks.md)

## Kanban и задачи

В рамках задания необходимо создать Kanban‑доску GitHub Projects и завести ~5 задач со статусом «Новые». Ниже — готовые команды для GitHub CLI.

Сначала аутентифицируйтесь:

```bash
gh auth login
```

Создайте Project (user‑level) и колонки Kanban:

```bash
# создаст проект и вернёт URL; сохраните номер/ID
gh project create --title "Anti‑Fraud MVP" --format json | jq -r '.url'

# создать базовые колонки Kanban (To do, In progress, Done) если нужен классический ProjectV1
# Для ProjectV2 (рекомендуется) используются настраиваемые поля; колонки не требуются.
```

Создайте задачи (Issues) со статусом «Новые» и привяжите к проекту:

```bash
# Замените <OWNER/REPO> на ваш репозиторий, <PROJECT_NUMBER> на номер проекта

gh issue create --title "Подготовка данных и EDA" --body "Собрать исторические CSV, провести EDA, подготовить датасет с time‑split. См. docs/04_smart_tasks.md" --repo <OWNER/REPO>
gh issue create --title "Базовая модель и метрики" --body "Обучить бустинг, калибровка, Recall@FPR≤5%, PR‑AUC. См. docs/04_smart_tasks.md" --repo <OWNER/REPO>
gh issue create --title "Онлайн‑скоринг сервис (MVP)" --body "REST API, p95 ≤ 150 мс при 400 RPS, логирование без PII." --repo <OWNER/REPO>
gh issue create --title "Мониторинг качества и дрейфа" --body "Evidently, алерты по drift, дашборды, SLO." --repo <OWNER/REPO>
gh issue create --title "Безопасность и комплаенс PII" --body "Псевдонимизация, шифрование, секрет‑менеджмент, RBAC." --repo <OWNER/REPO>

# Привязка к ProjectV2 (нужен ID проекта)
# Получить ID: gh project view <NUMBER> --format json | jq -r '.id'
# Получить ID issue: gh issue view <NUM> --json id --jq .id
# Добавить карточки: gh project item-add --project-id <PROJECT_ID> --item-id <ISSUE_ID>
```

Если GitHub CLI недоступен, создайте Project и Issues вручную через веб‑интерфейс GitHub и используйте тексты из `docs/04_smart_tasks.md`.

