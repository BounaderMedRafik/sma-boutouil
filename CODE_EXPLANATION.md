# Code Explanation: Multi-Agent Calculator Demo

## Overview

This project demonstrates a simple **multi-agent system** built with Python and `asyncio`. It simulates two agents communicating through a shared in-memory message bus:

- **ClientAgent**: sends calculation requests
- **CalculatorAgent**: receives requests, validates them, performs addition, and returns responses

The code follows a lightweight **request/response** model inspired by agent communication standards such as **FIPA-ACL**.

---

## Core Functionalities

### 1. Standardized Message Format

The `ACLMessage` dataclass defines a common structure for all messages exchanged between agents. Each message contains:

- `sender`: who sent the message
- `receiver`: who should receive it
- `content`: the payload, usually JSON text
- `performative`: the intent of the message, such as request or inform

This makes communication consistent and easier to manage.

### 2. Message Types with Performative

The `Performative` enum defines the meaning of a message:

- `REQUEST`: asks another agent to do something
- `INFORM`: provides successful information or a result
- `REFUSE`: indicates malformed or invalid input
- `FAILURE`: indicates a request could not be processed

These values help the agents understand how to interpret a message.

### 3. Central Message Routing

The `MessageBus` class acts like a central router. It stores one queue per agent and delivers messages to the correct recipient.

Main responsibilities:

- register agents
- unregister agents
- send messages to a target agent
- receive messages with timeout support

This avoids direct coupling between agents and keeps communication organized.

### 4. Base Agent Behavior

The `SimpleAgent` class provides shared behavior for all agents:

- starting and stopping the agent
- receiving messages in a background loop
- sending messages through the bus
- handling messages through an overridable method

It is designed as a reusable parent class so that specialized agents can focus on their own logic.

### 5. Calculator Logic

The `CalculatorAgent` class extends `SimpleAgent` and implements the main business logic.

When it receives a request:

1. It checks that the performative is `REQUEST`
2. It parses the JSON content
3. It verifies that keys `a` and `b` exist
4. It checks that both values are numeric
5. It adds the two numbers
6. It sends back a response message

It also handles errors:

- invalid JSON → returns `REFUSE`
- missing keys or invalid types → returns `FAILURE`

### 6. Client Request Handling

The `ClientAgent` class sends calculation requests and processes responses.

It has a helper method, `send_calculation(a, b)`, which:

- builds a JSON request
- wraps it in an `ACLMessage`
- sends it to the calculator agent

Its `handle_message` method interprets the calculator's reply:

- `INFORM` → prints the calculation result
- `REFUSE` → prints a JSON/format error
- `FAILURE` → prints a validation error

### 7. Demonstration Flow

The `main()` function runs a full demo of the system:

- creates the message bus
- creates the calculator and client agents
- starts both agents
- sends several test messages
- waits briefly after each test so messages can be processed
- stops both agents cleanly

The demo includes both valid and invalid cases to show how the system behaves under different inputs.

---

## Key Test Scenarios

The demonstration includes five scenarios:

1. **Valid calculation**: `5 + 3`
2. **Another valid calculation**: `12.5 + 7.3`
3. **Missing field**: JSON without `b`
4. **Malformed JSON**: invalid JSON string
5. **Non-numeric value**: `10 + "twenty"`

These cases help verify both normal processing and error handling.

---

## Automated Test Function

The `run_test_scenarios()` function provides extra programmatic tests.

It creates a separate bus and agents, then sends additional valid cases such as:

- integers
- floats
- negative numbers
- zero values
- very large numbers

This function is useful for expanding test coverage without changing the main demo flow.

---

## Design Highlights

### Asynchronous Communication

The code uses `asyncio` so agents can run concurrently without blocking each other. This is important in message-driven systems where one agent may wait for input while another processes messages.

### Loose Coupling

Agents do not call each other directly. They communicate through the `MessageBus`, which makes the design cleaner and easier to extend.

### Error Handling

The calculator agent distinguishes between different failure cases, which improves reliability and makes debugging easier.

### Extensibility

This structure can be extended to support:

- subtraction, multiplication, division
- multiple calculators
- more agent roles
- richer ACL-style message exchanges

---

## Summary

This code is a compact example of a Python-based multi-agent system. It demonstrates how to:

- define structured messages
- route messages through a shared bus
- run agents asynchronously
- process requests and responses
- handle both success and error cases

In short, it is a small but clear example of agent communication, validation, and asynchronous workflow design.
