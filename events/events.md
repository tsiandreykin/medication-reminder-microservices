
# Kafka Events

## 1. Общий формат события

Все бизнес-события системы передаются через Apache Kafka в едином формате.

Каждое событие содержит метаданные и полезную нагрузку `payload`.

```json
{
  "eventId": "3f6c2b0e-8d1a-4c57-9e2f-5a1b7c9d0e44",
  "correlationId": "7a9d2e1f-4b6c-4d8a-9f3e-2c1b5a6d7e80",
  "eventType": "PrescriptionCreated",
  "schemaVersion": 1,
  "occurredAt": "2026-11-24T10:30:00Z",
  "payload": {}
}
```

### Метаданные

| Поле | Тип | Назначение |
|---|---|---|
| `eventId` | UUID | Уникальный идентификатор события |
| `correlationId` | UUID | Идентификатор бизнес-сценария, связывающий связанные события |
| `eventType` | string | Название события |
| `schemaVersion` | integer | Версия структуры события |
| `occurredAt` | date-time | Время возникновения события |
| `payload` | object | Данные события |

`eventId` используется для обнаружения повторной доставки. `correlationId` связывает события, относящиеся к одному бизнес-сценарию. `schemaVersion` позволяет изменять структуру событий с сохранением совместимости между версиями.

## 2. Kafka Topics

Для обмена событиями используются следующие топики:

| Topic | Издатель | Основные подписчики |
|---|---|---|
| `patient.events` | `patient-service` | `report-service` |
| `medicine.events` | `medicine-service` | На текущем этапе подписчики не определены |
| `prescription.events` | `prescription-service` | `schedule-service`, `report-service` |
| `schedule.events` | `schedule-service` | `notification-service`, `report-service` |
| `notification.events` | `notification-service` | На текущем этапе подписчики не определены |

Ключ сообщения Kafka выбирается по идентификатору основной сущности события. Это позволяет сохранять порядок сообщений, относящихся к одной сущности, в пределах раздела топика.

## 3. События patient-service

### PatientRegistered

**Назначение:** пациент зарегистрирован в системе.

- Topic: `patient.events`
- Publisher: `patient-service`
- Subscribers: `report-service`

Payload:

```json
{
  "patientId": 101,
  "fullName": "Иванов Иван Иванович",
  "doctorId": 15
}
```

Kafka key: `patientId`.

### PatientUpdated

**Назначение:** сведения о пациенте изменены.

- Topic: `patient.events`
- Publisher: `patient-service`
- Subscribers: `report-service`

Payload:

```json
{
  "patientId": 101,
  "fullName": "Иванов Иван Иванович",
  "doctorId": 15
}
```

Kafka key: `patientId`.

### DoctorRegistered

**Назначение:** врач зарегистрирован в системе.

- Topic: `patient.events`
- Publisher: `patient-service`
- Subscribers: на текущем этапе не определены.

Payload:

```json
{
  "doctorId": 15
}
```

Kafka key: `doctorId`.

## 4. События medicine-service

### MedicineCreated

**Назначение:** новый препарат добавлен в справочник.

- Topic: `medicine.events`
- Publisher: `medicine-service`
- Subscribers: на текущем этапе не определены.

Payload:

```json
{
  "medicineId": 201,
  "name": "Препарат А",
  "manufacturer": "ФармТек",
  "description": "Описание препарата",
  "dosageForm": "tablets",
  "activeSubstance": "Вещество А",
  "isActive": true
}
```

Kafka key: `medicineId`.

### MedicineUpdated

**Назначение:** сведения о препарате изменены.

- Topic: `medicine.events`
- Publisher: `medicine-service`
- Subscribers: на текущем этапе не определены.

Payload:

```json
{
  "medicineId": 201,
  "name": "Препарат А",
  "manufacturer": "ФармТек",
  "description": "Обновлённое описание",
  "dosageForm": "tablets",
  "activeSubstance": "Вещество А",
  "isActive": true
}
```

Kafka key: `medicineId`.

### MedicineDeactivated

**Назначение:** препарат помечен как неактивный.

- Topic: `medicine.events`
- Publisher: `medicine-service`
- Subscribers: на текущем этапе не определены.

Payload:

```json
{
  "medicineId": 201
}
```

Kafka key: `medicineId`.

## 5. События prescription-service

### PrescriptionCreated

**Назначение:** врач создал новое назначение. Событие используется для формирования расписания.

- Topic: `prescription.events`
- Publisher: `prescription-service`
- Subscribers: `schedule-service`, `report-service`

Payload:

```json
{
  "prescriptionId": 301,
  "patientId": 101,
  "doctorId": 15,
  "medicineId": 201,
  "dosage": "1 таблетка",
  "frequency": "2 раза в день",
  "startDate": "2026-11-24",
  "endDate": "2026-12-24",
  "instructions": "Принимать после еды",
  "status": "active"
}
```

Kafka key: `prescriptionId`.

### PrescriptionUpdated

**Назначение:** параметры существующего назначения изменены.

- Topic: `prescription.events`
- Publisher: `prescription-service`
- Subscribers: `schedule-service`, `report-service`

Payload:

```json
{
  "prescriptionId": 301,
  "patientId": 101,
  "doctorId": 15,
  "medicineId": 201,
  "dosage": "1 таблетка",
  "frequency": "1 раз в день",
  "startDate": "2026-11-24",
  "endDate": "2026-12-24",
  "instructions": "Принимать после еды",
  "status": "active"
}
```

Kafka key: `prescriptionId`.

### PrescriptionCancelled

**Назначение:** назначение отменено.

- Topic: `prescription.events`
- Publisher: `prescription-service`
- Subscribers: `schedule-service`, `report-service`

Payload:

```json
{
  "prescriptionId": 301,
  "patientId": 101,
  "doctorId": 15,
  "medicineId": 201,
  "status": "cancelled"
}
```

Kafka key: `prescriptionId`.

## 6. События schedule-service

### ScheduleGenerated

**Назначение:** расписание сформировано на основании назначения.

- Topic: `schedule.events`
- Publisher: `schedule-service`
- Subscribers: `notification-service`, `report-service`

Payload:

```json
{
  "scheduleEntryId": 401,
  "prescriptionId": 301,
  "patientId": 101,
  "medicineId": 201,
  "scheduledAt": "2026-11-24T09:00:00Z",
  "status": "scheduled"
}
```

Kafka key: `scheduleEntryId`.

### ScheduleUpdated

**Назначение:** будущие записи расписания изменены.

- Topic: `schedule.events`
- Publisher: `schedule-service`
- Subscribers: `notification-service`

Payload:

```json
{
  "scheduleEntryId": 401,
  "prescriptionId": 301,
  "patientId": 101,
  "medicineId": 201,
  "scheduledAt": "2026-11-24T10:00:00Z",
  "status": "scheduled"
}
```

Kafka key: `scheduleEntryId`.

### ScheduleCancelled

**Назначение:** будущая запись расписания отменена.

- Topic: `schedule.events`
- Publisher: `schedule-service`
- Subscribers: `notification-service`, `report-service` при необходимости актуализации запланированных приёмов.

Payload:

```json
{
  "scheduleEntryId": 401,
  "prescriptionId": 301,
  "patientId": 101,
  "medicineId": 201,
  "scheduledAt": "2026-11-24T09:00:00Z",
  "status": "cancelled"
}
```

Kafka key: `scheduleEntryId`.

### MedicationTaken

**Назначение:** пациент подтвердил приём лекарства.

- Topic: `schedule.events`
- Publisher: `schedule-service`
- Subscribers: `report-service`

Payload:

```json
{
  "scheduleEntryId": 401,
  "prescriptionId": 301,
  "patientId": 101,
  "medicineId": 201,
  "scheduledAt": "2026-11-24T09:00:00Z",
  "takenAt": "2026-11-24T09:05:00Z",
  "status": "taken"
}
```

Kafka key: `scheduleEntryId`.

### MedicationMissed

**Назначение:** запланированный приём признан пропущенным.

- Topic: `schedule.events`
- Publisher: `schedule-service`
- Subscribers: `report-service`

Payload:

```json
{
  "scheduleEntryId": 401,
  "prescriptionId": 301,
  "patientId": 101,
  "medicineId": 201,
  "scheduledAt": "2026-11-24T09:00:00Z",
  "status": "missed"
}
```

Kafka key: `scheduleEntryId`.

## 7. События notification-service

### ReminderSent

**Назначение:** уведомление успешно передано провайдеру доставки.

- Topic: `notification.events`
- Publisher: `notification-service`
- Subscribers: на текущем этапе не определены.

Payload:

```json
{
  "notificationId": 501,
  "scheduleEntryId": 401,
  "patientId": 101,
  "result": "success"
}
```

Kafka key: `notificationId`.

### ReminderDeliveryFailed

**Назначение:** доставка уведомления завершилась окончательной ошибкой после исчерпания допустимых повторных попыток.

- Topic: `notification.events`
- Publisher: `notification-service`
- Subscribers: на текущем этапе не определены.

Payload:

```json
{
  "notificationId": 501,
  "scheduleEntryId": 401,
  "patientId": 101,
  "result": "permanent_failure"
}
```

Kafka key: `notificationId`.

## 8. Надёжность и обработка событий

Для взаимодействия через Kafka предусматриваются следующие правила:

1. **Идемпотентность.** Каждый потребитель сохраняет обработанные `eventId`. Повторная доставка одного события не должна повторно изменять данные или создавать дубликаты.

2. **Повторная доставка.** При временной ошибке обработки сообщение может быть обработано повторно. Повторные попытки должны быть ограничены.

3. **Порядок сообщений.** В качестве Kafka key используется идентификатор основной сущности события. Это помогает сохранять порядок событий одной сущности в пределах раздела топика.

4. **Transactional Outbox.** Для публикации событий сервис сначала сохраняет изменение данных и событие в собственной базе данных в одной транзакции. Отдельный процесс передаёт событие в Kafka.

5. **Dead Letter Queue.** Сообщения, которые не удалось обработать после допустимого количества повторов, могут направляться в отдельную очередь для последующего анализа.

6. **Версионирование.** Изменение структуры события сопровождается увеличением `schemaVersion`. Изменения не должны неожиданно нарушать работу существующих подписчиков.

7. **Минимизация данных.** События не содержат пароли, токены, контактные данные и другие лишние персональные сведения.

## 9. Формальные JSON Schema

Ниже приведена единая JSON Schema (Draft 2020-12) для конвертов всех 16 событий и их `payload`.
Для каждого события зафиксированы `eventType`, обязательные метаданные и структура полезной нагрузки.

Схемы payload составлены по JSON-примерам, уже приведённым в этом документе. Типы идентификаторов приведены к согласованному формату `integer/int64`; даты и временные метки используют форматы `date` и `date-time`. Новые бизнес-поля не добавлены.

`correlationId` — техническая метаинформация сообщения Kafka. Он не является полем бизнес-сущности и не требует изменения базы данных сервиса.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "urn:medication-reminder:kafka-events:v1",
  "title": "Kafka event envelopes",
  "oneOf": [
    {
      "$ref": "#/$defs/PatientRegisteredEvent"
    },
    {
      "$ref": "#/$defs/PatientUpdatedEvent"
    },
    {
      "$ref": "#/$defs/DoctorRegisteredEvent"
    },
    {
      "$ref": "#/$defs/MedicineCreatedEvent"
    },
    {
      "$ref": "#/$defs/MedicineUpdatedEvent"
    },
    {
      "$ref": "#/$defs/MedicineDeactivatedEvent"
    },
    {
      "$ref": "#/$defs/PrescriptionCreatedEvent"
    },
    {
      "$ref": "#/$defs/PrescriptionUpdatedEvent"
    },
    {
      "$ref": "#/$defs/PrescriptionCancelledEvent"
    },
    {
      "$ref": "#/$defs/ScheduleGeneratedEvent"
    },
    {
      "$ref": "#/$defs/ScheduleUpdatedEvent"
    },
    {
      "$ref": "#/$defs/ScheduleCancelledEvent"
    },
    {
      "$ref": "#/$defs/MedicationTakenEvent"
    },
    {
      "$ref": "#/$defs/MedicationMissedEvent"
    },
    {
      "$ref": "#/$defs/ReminderSentEvent"
    },
    {
      "$ref": "#/$defs/ReminderDeliveryFailedEvent"
    }
  ],
  "$defs": {
    "PatientRegisteredEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "PatientRegistered"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/PatientRegisteredPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "PatientUpdatedEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "PatientUpdated"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/PatientUpdatedPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "DoctorRegisteredEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "DoctorRegistered"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/DoctorRegisteredPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "MedicineCreatedEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "MedicineCreated"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/MedicineCreatedPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "MedicineUpdatedEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "MedicineUpdated"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/MedicineUpdatedPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "MedicineDeactivatedEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "MedicineDeactivated"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/MedicineDeactivatedPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "PrescriptionCreatedEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "PrescriptionCreated"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/PrescriptionCreatedPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "PrescriptionUpdatedEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "PrescriptionUpdated"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/PrescriptionUpdatedPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "PrescriptionCancelledEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "PrescriptionCancelled"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/PrescriptionCancelledPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "ScheduleGeneratedEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "ScheduleGenerated"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/ScheduleGeneratedPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "ScheduleUpdatedEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "ScheduleUpdated"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/ScheduleUpdatedPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "ScheduleCancelledEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "ScheduleCancelled"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/ScheduleCancelledPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "MedicationTakenEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "MedicationTaken"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/MedicationTakenPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "MedicationMissedEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "MedicationMissed"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/MedicationMissedPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "ReminderSentEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "ReminderSent"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/ReminderSentPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "ReminderDeliveryFailedEvent": {
      "type": "object",
      "properties": {
        "eventId": {
          "type": "string",
          "format": "uuid"
        },
        "correlationId": {
          "type": "string",
          "format": "uuid"
        },
        "eventType": {
          "const": "ReminderDeliveryFailed"
        },
        "schemaVersion": {
          "type": "integer",
          "minimum": 1
        },
        "occurredAt": {
          "type": "string",
          "format": "date-time"
        },
        "payload": {
          "$ref": "#/$defs/ReminderDeliveryFailedPayload"
        }
      },
      "required": [
        "eventId",
        "correlationId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "payload"
      ]
    },
    "PatientRegisteredPayload": {
      "type": "object",
      "properties": {
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "fullName": {
          "type": "string"
        },
        "doctorId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        }
      },
      "required": [
        "patientId",
        "fullName",
        "doctorId"
      ]
    },
    "PatientUpdatedPayload": {
      "type": "object",
      "properties": {
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "fullName": {
          "type": "string"
        },
        "doctorId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        }
      },
      "required": [
        "patientId",
        "fullName",
        "doctorId"
      ]
    },
    "DoctorRegisteredPayload": {
      "type": "object",
      "properties": {
        "doctorId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        }
      },
      "required": [
        "doctorId"
      ]
    },
    "MedicineCreatedPayload": {
      "type": "object",
      "properties": {
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "name": {
          "type": "string"
        },
        "manufacturer": {
          "type": "string"
        },
        "description": {
          "type": "string"
        },
        "dosageForm": {
          "type": "string"
        },
        "activeSubstance": {
          "type": "string"
        },
        "isActive": {
          "type": "boolean"
        }
      },
      "required": [
        "medicineId",
        "name",
        "manufacturer",
        "description",
        "dosageForm",
        "activeSubstance",
        "isActive"
      ]
    },
    "MedicineUpdatedPayload": {
      "type": "object",
      "properties": {
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "name": {
          "type": "string"
        },
        "manufacturer": {
          "type": "string"
        },
        "description": {
          "type": "string"
        },
        "dosageForm": {
          "type": "string"
        },
        "activeSubstance": {
          "type": "string"
        },
        "isActive": {
          "type": "boolean"
        }
      },
      "required": [
        "medicineId",
        "name",
        "manufacturer",
        "description",
        "dosageForm",
        "activeSubstance",
        "isActive"
      ]
    },
    "MedicineDeactivatedPayload": {
      "type": "object",
      "properties": {
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        }
      },
      "required": [
        "medicineId"
      ]
    },
    "PrescriptionCreatedPayload": {
      "type": "object",
      "properties": {
        "prescriptionId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "doctorId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "dosage": {
          "type": "string"
        },
        "frequency": {
          "type": "string"
        },
        "startDate": {
          "type": "string",
          "format": "date"
        },
        "endDate": {
          "type": "string",
          "format": "date"
        },
        "instructions": {
          "type": "string"
        },
        "status": {
          "type": "string",
          "enum": [
            "active",
            "cancelled",
            "completed"
          ]
        }
      },
      "required": [
        "prescriptionId",
        "patientId",
        "doctorId",
        "medicineId",
        "dosage",
        "frequency",
        "startDate",
        "endDate",
        "instructions",
        "status"
      ]
    },
    "PrescriptionUpdatedPayload": {
      "type": "object",
      "properties": {
        "prescriptionId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "doctorId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "dosage": {
          "type": "string"
        },
        "frequency": {
          "type": "string"
        },
        "startDate": {
          "type": "string",
          "format": "date"
        },
        "endDate": {
          "type": "string",
          "format": "date"
        },
        "instructions": {
          "type": "string"
        },
        "status": {
          "type": "string",
          "enum": [
            "active",
            "cancelled",
            "completed"
          ]
        }
      },
      "required": [
        "prescriptionId",
        "patientId",
        "doctorId",
        "medicineId",
        "dosage",
        "frequency",
        "startDate",
        "endDate",
        "instructions",
        "status"
      ]
    },
    "PrescriptionCancelledPayload": {
      "type": "object",
      "properties": {
        "prescriptionId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "doctorId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "status": {
          "type": "string",
          "enum": [
            "active",
            "cancelled",
            "completed"
          ]
        }
      },
      "required": [
        "prescriptionId",
        "patientId",
        "doctorId",
        "medicineId",
        "status"
      ]
    },
    "ScheduleGeneratedPayload": {
      "type": "object",
      "properties": {
        "scheduleEntryId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "prescriptionId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "scheduledAt": {
          "type": "string",
          "format": "date-time"
        },
        "status": {
          "type": "string",
          "enum": [
            "scheduled",
            "taken",
            "missed",
            "cancelled"
          ]
        }
      },
      "required": [
        "scheduleEntryId",
        "prescriptionId",
        "patientId",
        "medicineId",
        "scheduledAt",
        "status"
      ]
    },
    "ScheduleUpdatedPayload": {
      "type": "object",
      "properties": {
        "scheduleEntryId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "prescriptionId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "scheduledAt": {
          "type": "string",
          "format": "date-time"
        },
        "status": {
          "type": "string",
          "enum": [
            "scheduled",
            "taken",
            "missed",
            "cancelled"
          ]
        }
      },
      "required": [
        "scheduleEntryId",
        "prescriptionId",
        "patientId",
        "medicineId",
        "scheduledAt",
        "status"
      ]
    },
    "ScheduleCancelledPayload": {
      "type": "object",
      "properties": {
        "scheduleEntryId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "prescriptionId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "scheduledAt": {
          "type": "string",
          "format": "date-time"
        },
        "status": {
          "type": "string",
          "enum": [
            "scheduled",
            "taken",
            "missed",
            "cancelled"
          ]
        }
      },
      "required": [
        "scheduleEntryId",
        "prescriptionId",
        "patientId",
        "medicineId",
        "scheduledAt",
        "status"
      ]
    },
    "MedicationTakenPayload": {
      "type": "object",
      "properties": {
        "scheduleEntryId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "prescriptionId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "scheduledAt": {
          "type": "string",
          "format": "date-time"
        },
        "takenAt": {
          "type": "string",
          "format": "date-time"
        },
        "status": {
          "type": "string",
          "enum": [
            "scheduled",
            "taken",
            "missed",
            "cancelled"
          ]
        }
      },
      "required": [
        "scheduleEntryId",
        "prescriptionId",
        "patientId",
        "medicineId",
        "scheduledAt",
        "takenAt",
        "status"
      ]
    },
    "MedicationMissedPayload": {
      "type": "object",
      "properties": {
        "scheduleEntryId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "prescriptionId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "medicineId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "scheduledAt": {
          "type": "string",
          "format": "date-time"
        },
        "status": {
          "type": "string",
          "enum": [
            "scheduled",
            "taken",
            "missed",
            "cancelled"
          ]
        }
      },
      "required": [
        "scheduleEntryId",
        "prescriptionId",
        "patientId",
        "medicineId",
        "scheduledAt",
        "status"
      ]
    },
    "ReminderSentPayload": {
      "type": "object",
      "properties": {
        "notificationId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "scheduleEntryId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "result": {
          "type": "string",
          "enum": [
            "success"
          ]
        }
      },
      "required": [
        "notificationId",
        "scheduleEntryId",
        "patientId",
        "result"
      ]
    },
    "ReminderDeliveryFailedPayload": {
      "type": "object",
      "properties": {
        "notificationId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "scheduleEntryId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "patientId": {
          "type": "integer",
          "format": "int64",
          "minimum": 1
        },
        "result": {
          "type": "string",
          "enum": [
            "permanent_failure"
          ]
        }
      },
      "required": [
        "notificationId",
        "scheduleEntryId",
        "patientId",
        "result"
      ]
    }
  }
}
```

## 10. Сводная таблица событий

| Событие | Издатель | Подписчики |
|---|---|---|
| `PatientRegistered` | patient-service | report-service |
| `PatientUpdated` | patient-service | report-service |
| `DoctorRegistered` | patient-service | — |
| `MedicineCreated` | medicine-service | — |
| `MedicineUpdated` | medicine-service | — |
| `MedicineDeactivated` | medicine-service | — |
| `PrescriptionCreated` | prescription-service | schedule-service, report-service |
| `PrescriptionUpdated` | prescription-service | schedule-service, report-service |
| `PrescriptionCancelled` | prescription-service | schedule-service, report-service |
| `ScheduleGenerated` | schedule-service | notification-service, report-service |
| `ScheduleUpdated` | schedule-service | notification-service |
| `ScheduleCancelled` | schedule-service | notification-service, report-service |
| `MedicationTaken` | schedule-service | report-service |
| `MedicationMissed` | schedule-service | report-service |
| `ReminderSent` | notification-service | — |
| `ReminderDeliveryFailed` | notification-service | — |

`report-service` не публикует бизнес-события в текущей архитектуре.
