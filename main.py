import asyncio
import json
import threading
from queue import Queue, Empty
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum

try:
    import tkinter as tk
    from tkinter import ttk, messagebox
except ImportError:  # pragma: no cover - Tkinter may be unavailable in some environments
    tk = None
    ttk = None
    messagebox = None


LogFn = Callable[[str], None]


class Performative(Enum):
    """FIPA-ACL performatives for message types"""
    REQUEST = "request"
    INFORM = "inform"
    REFUSE = "refuse"
    FAILURE = "failure"


@dataclass
class ACLMessage:
    """Standardized message format following FIPA-ACL"""
    sender: str
    receiver: str
    content: Any
    performative: Performative = Performative.REQUEST
    
    def to_dict(self) -> Dict:
        return {
            "sender": self.sender,
            "receiver": self.receiver,
            "content": self.content,
            "performative": self.performative.value
        }
    
    @staticmethod
    def from_dict(data: Dict) -> 'ACLMessage':
        return ACLMessage(
            sender=data["sender"],
            receiver=data["receiver"],
            content=data["content"],
            performative=Performative(data["performative"])
        )


class MessageBus:
    """Centralized message router managing queues for each agent"""
    
    def __init__(self, logger: Optional[LogFn] = None):
        self.queues: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()
        self._log = logger or print
    
    async def register(self, agent_id: str) -> None:
        """Register an agent and create its message queue"""
        async with self._lock:
            if agent_id not in self.queues:
                self.queues[agent_id] = asyncio.Queue()
                self._log(f"[Bus] Agent '{agent_id}' registered")
    
    async def unregister(self, agent_id: str) -> None:
        """Unregister an agent and remove its queue"""
        async with self._lock:
            if agent_id in self.queues:
                del self.queues[agent_id]
                self._log(f"[Bus] Agent '{agent_id}' unregistered")
    
    async def send(self, message: ACLMessage) -> bool:
        """Route a message to the recipient's queue"""
        if message.receiver not in self.queues:
            self._log(f"[Bus] Error: Recipient '{message.receiver}' not found")
            return False
        
        await self.queues[message.receiver].put(message)
        self._log(f"[Bus] Message from '{message.sender}' to '{message.receiver}' routed")
        return True
    
    async def receive(self, agent_id: str, timeout: float = 1.0) -> Optional[ACLMessage]:
        """Receive a message for a specific agent with timeout"""
        if agent_id not in self.queues:
            return None
        
        try:
            message = await asyncio.wait_for(self.queues[agent_id].get(), timeout=timeout)
            return message
        except asyncio.TimeoutError:
            return None


class SimpleAgent:
    """Base agent class providing core communication capabilities"""
    
    def __init__(self, jid: str, bus: MessageBus, logger: Optional[LogFn] = None):
        self.jid = jid
        self.bus = bus
        self.running = False
        self._receive_task: Optional[asyncio.Task] = None
        self.log = logger or print
    
    async def start(self) -> None:
        """Start the agent: register with bus and begin receiving messages"""
        await self.bus.register(self.jid)
        self.running = True
        self._receive_task = asyncio.create_task(self._receive_loop())
        self.log(f"[{self.jid}] Agent started")
    
    async def stop(self) -> None:
        """Stop the agent gracefully"""
        self.running = False
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        await self.bus.unregister(self.jid)
        self.log(f"[{self.jid}] Agent stopped")
    
    async def _receive_loop(self) -> None:
        """Internal loop that continuously calls receive()"""
        while self.running:
            message = await self.receive(timeout=1.0)
            if message:
                await self.handle_message(message)
    
    async def receive(self, timeout: float = 1.0) -> Optional[ACLMessage]:
        """Wait for and return a message addressed to this agent"""
        return await self.bus.receive(self.jid, timeout)
    
    async def send(self, message: ACLMessage) -> bool:
        """Send a message via the message bus"""
        return await self.bus.send(message)
    
    async def handle_message(self, message: ACLMessage) -> None:
        """Override this method in child classes to handle incoming messages"""
        pass


class CalculatorAgent(SimpleAgent):
    """Agent that performs addition operations upon request"""

    def __init__(self, jid: str, bus: MessageBus, logger: Optional[LogFn] = None):
        super().__init__(jid, bus, logger)
    
    async def handle_message(self, message: ACLMessage) -> None:
        """Process calculation requests and send results"""
        if message.performative != Performative.REQUEST:
            return
        
        try:
            # Parse the JSON content
            data = json.loads(message.content)
            
            # Validate required keys
            if "a" not in data or "b" not in data:
                raise ValueError("Missing required keys: 'a' and 'b'")
            
            # Ensure values are numbers
            if not isinstance(data["a"], (int, float)) or not isinstance(data["b"], (int, float)):
                raise ValueError("Values 'a' and 'b' must be numbers")
            
            # Perform calculation
            result = data["a"] + data["b"]
            
            # Send success response
            response = ACLMessage(
                sender=self.jid,
                receiver=message.sender,
                content=json.dumps({"result": result, "status": "success"}),
                performative=Performative.INFORM
            )
            await self.send(response)
            self.log(f"[{self.jid}] Calculated {data['a']} + {data['b']} = {result}")
            
        except json.JSONDecodeError as e:
            # Malformed JSON
            response = ACLMessage(
                sender=self.jid,
                receiver=message.sender,
                content=json.dumps({"error": "Invalid JSON format", "details": str(e)}),
                performative=Performative.REFUSE
            )
            await self.send(response)
            self.log(f"[{self.jid}] Refused request: Invalid JSON")
            
        except ValueError as e:
            # Missing keys or invalid values
            response = ACLMessage(
                sender=self.jid,
                receiver=message.sender,
                content=json.dumps({"error": str(e)}),
                performative=Performative.FAILURE
            )
            await self.send(response)
            self.log(f"[{self.jid}] Failed request: {e}")

    async def receive(self, timeout: float = 1.0) -> Optional[ACLMessage]:
        """Override receive with timeout to allow checking running state"""
        return await self.bus.receive(self.jid, timeout)


class ClientAgent(SimpleAgent):
    """Client agent that sends calculation requests and displays results"""
    
    def __init__(self, jid: str, bus: MessageBus, calculator_jid: str, logger: Optional[LogFn] = None):
        super().__init__(jid, bus, logger)
        self.calculator_jid = calculator_jid
        self.result_received = False
    
    async def send_calculation(self, a: float, b: float) -> None:
        """Send a calculation request to the calculator agent"""
        request_content = json.dumps({"a": a, "b": b})
        
        message = ACLMessage(
            sender=self.jid,
            receiver=self.calculator_jid,
            content=request_content,
            performative=Performative.REQUEST
        )
        
        await self.send(message)
        self.log(f"[{self.jid}] Sent calculation request: {a} + {b}")
    
    async def handle_message(self, message: ACLMessage) -> None:
        """Handle response messages from the calculator"""
        try:
            data = json.loads(message.content)
            
            if message.performative == Performative.INFORM:
                # Successful calculation
                self.log(f"[{self.jid}] Received result: {data['result']}")
                self.result_received = True
                
            elif message.performative == Performative.REFUSE:
                self.log(f"[{self.jid}] Request refused: {data.get('error', 'Unknown error')}")
                
            elif message.performative == Performative.FAILURE:
                self.log(f"[{self.jid}] Request failed: {data.get('error', 'Unknown error')}")
                
        except json.JSONDecodeError:
            self.log(f"[{self.jid}] Received malformed response")


class AgentRuntime:
    """Run the async message bus and agents on a dedicated background thread."""

    def __init__(self, logger: LogFn):
        self.logger = logger
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None
        self.bus: Optional[MessageBus] = None
        self.calculator: Optional[CalculatorAgent] = None
        self.client: Optional[ClientAgent] = None
        self._ready = threading.Event()
        self._startup_error: Optional[BaseException] = None

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return

        self._ready.clear()
        self._startup_error = None
        self.thread = threading.Thread(target=self._thread_main, daemon=True)
        self.thread.start()
        self._ready.wait(timeout=5)
        if self._startup_error:
            raise self._startup_error

    def _thread_main(self) -> None:
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.bus = MessageBus(logger=self.logger)
            self.calculator = CalculatorAgent("calculator@localhost", self.bus, logger=self.logger)
            self.client = ClientAgent("client@localhost", self.bus, "calculator@localhost", logger=self.logger)

            async def bootstrap() -> None:
                await self.calculator.start()
                await self.client.start()

            self.loop.run_until_complete(bootstrap())
            self._ready.set()
            self.loop.run_forever()
        except BaseException as exc:  # pragma: no cover - surfaced in GUI log
            self._startup_error = exc
            self._ready.set()
        finally:
            if self.loop and not self.loop.is_closed():
                pending = asyncio.all_tasks(loop=self.loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                self.loop.close()

    def send_calculation(self, a: float, b: float) -> None:
        if not self.loop or not self.client:
            raise RuntimeError("Agents are not running")

        future = asyncio.run_coroutine_threadsafe(self.client.send_calculation(a, b), self.loop)
        future.result(timeout=5)

    def stop(self) -> None:
        if not self.loop or not self.thread or not self.thread.is_alive():
            return

        async def shutdown() -> None:
            if self.client:
                await self.client.stop()
            if self.calculator:
                await self.calculator.stop()
            self.loop.stop()

        future = asyncio.run_coroutine_threadsafe(shutdown(), self.loop)
        try:
            future.result(timeout=5)
        except Exception:
            pass
        self.thread.join(timeout=5)


class CalculatorGUI:
    """Tkinter GUI for interacting with the calculator agents."""

    def __init__(self):
        if tk is None:
            raise RuntimeError("Tkinter is not available in this environment")

        self.root = tk.Tk()
        self.root.title("Multi-Agent Calculator")
        self.root.geometry("700x500")

        self.log_queue: Queue[str] = Queue()
        self.runtime = AgentRuntime(self.log_queue.put)

        self._build_ui()
        self.runtime.start()
        self._append_log("[GUI] Agents started")
        self._poll_logs()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        header = ttk.Label(main, text="Multi-Agent Calculator", font=("Segoe UI", 16, "bold"))
        header.pack(anchor="w", pady=(0, 12))

        input_frame = ttk.LabelFrame(main, text="Send Calculation", padding=10)
        input_frame.pack(fill=tk.X)

        ttk.Label(input_frame, text="A:").grid(row=0, column=0, sticky="w")
        self.a_var = tk.StringVar(value="5")
        ttk.Entry(input_frame, textvariable=self.a_var, width=15).grid(row=0, column=1, padx=(6, 18), pady=4)

        ttk.Label(input_frame, text="B:").grid(row=0, column=2, sticky="w")
        self.b_var = tk.StringVar(value="3")
        ttk.Entry(input_frame, textvariable=self.b_var, width=15).grid(row=0, column=3, padx=(6, 18), pady=4)

        ttk.Button(input_frame, text="Send Calculation", command=self.on_send).grid(row=0, column=4, padx=(6, 0))

        controls = ttk.Frame(main)
        controls.pack(fill=tk.X, pady=(10, 10))
        ttk.Button(controls, text="Stop Agents", command=self.on_stop_agents).pack(side=tk.LEFT)
        ttk.Button(controls, text="Start Agents", command=self.on_start_agents).pack(side=tk.LEFT, padx=8)
        ttk.Button(controls, text="Clear Log", command=self.on_clear_log).pack(side=tk.LEFT)

        log_frame = ttk.LabelFrame(main, text="Activity Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = tk.Text(log_frame, wrap=tk.WORD, height=18, state=tk.DISABLED)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.configure(yscrollcommand=scrollbar.set)

    def _append_log(self, message: str) -> None:
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _poll_logs(self) -> None:
        try:
            while True:
                message = self.log_queue.get_nowait()
                self._append_log(message)
        except Empty:
            pass
        self.root.after(100, self._poll_logs)

    def on_send(self) -> None:
        try:
            a = float(self.a_var.get())
            b = float(self.b_var.get())
            self.runtime.send_calculation(a, b)
        except ValueError:
            if messagebox:
                messagebox.showerror("Invalid Input", "Please enter numeric values for A and B.")
            else:
                self._append_log("[GUI] Invalid input: A and B must be numeric")
        except Exception as exc:
            self._append_log(f"[GUI] Error sending calculation: {exc}")

    def on_start_agents(self) -> None:
        try:
            self.runtime.start()
            self._append_log("[GUI] Agents are running")
        except Exception as exc:
            self._append_log(f"[GUI] Failed to start agents: {exc}")

    def on_stop_agents(self) -> None:
        try:
            self.runtime.stop()
            self._append_log("[GUI] Agents stopped")
        except Exception as exc:
            self._append_log(f"[GUI] Failed to stop agents: {exc}")

    def on_clear_log(self) -> None:
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def on_close(self) -> None:
        try:
            self.runtime.stop()
        finally:
            self.root.destroy()

    def run(self) -> None:
        self.root.mainloop()


async def main():
    """Main function to demonstrate the multi-agent system"""
    print("=" * 60)
    print("Multi-Agent System Demo - Calculator Client/Server")
    print("=" * 60)
    
    # Create the message bus
    bus = MessageBus()
    
    # Create agents
    calculator = CalculatorAgent("calculator@localhost", bus)
    client = ClientAgent("client@localhost", bus, "calculator@localhost")
    
    # Start agents
    await calculator.start()
    await client.start()
    
    print("\n--- Demonstration ---\n")
    
    # Test 1: Valid calculation
    print("Test 1: Valid calculation (5 + 3)")
    await client.send_calculation(5, 3)
    await asyncio.sleep(1)  # Wait for response
    
    # Test 2: Another valid calculation
    print("\nTest 2: Another valid calculation (12.5 + 7.3)")
    await client.send_calculation(12.5, 7.3)
    await asyncio.sleep(1)
    
    # Test 3: Invalid request (missing field)
    print("\nTest 3: Invalid request (missing 'b' field)")
    invalid_message = ACLMessage(
        sender=client.jid,
        receiver=calculator.jid,
        content=json.dumps({"a": 10}),  # Missing 'b'
        performative=Performative.REQUEST
    )
    await client.send(invalid_message)
    await asyncio.sleep(1)
    
    # Test 4: Malformed JSON
    print("\nTest 4: Malformed JSON")
    malformed_message = ACLMessage(
        sender=client.jid,
        receiver=calculator.jid,
        content="{invalid json",
        performative=Performative.REQUEST
    )
    await client.send(malformed_message)
    await asyncio.sleep(1)
    
    # Test 5: Non-numeric values
    print("\nTest 5: Non-numeric values")
    await client.send_calculation(10, "twenty")  # This will be handled by the try/except in handle_message
    await asyncio.sleep(1)
    
    print("\n--- Shutting down ---\n")
    
    # Stop agents
    await client.stop()
    await calculator.stop()
    
    # Give a moment for cleanup messages
    await asyncio.sleep(0.5)
    
    print("\nDemo completed!")


def run_test_scenarios():
    """Run additional test scenarios programmatically"""
    print("\n" + "=" * 60)
    print("Running automated test scenarios...")
    print("=" * 60)
    
    async def test_scenarios():
        bus = MessageBus()
        calculator = CalculatorAgent("calculator@test", bus)
        test_client = ClientAgent("tester@test", bus, "calculator@test")
        
        await calculator.start()
        await test_client.start()
        
        # Test valid calculations
        test_cases = [
            (10, 20, "Valid integers"),
            (3.14, 2.86, "Valid floats"),
            (-5, 15, "Negative numbers"),
            (0, 100, "Zero values"),
        ]
        
        for a, b, description in test_cases:
            print(f"\n{description}: {a} + {b}")
            await test_client.send_calculation(a, b)
            await asyncio.sleep(0.5)
        
        # Test edge cases
        print("\n--- Edge Cases ---")
        
        # Large numbers
        print("\nLarge numbers: 10**9 + 10**9")
        await test_client.send_calculation(10**9, 10**9)
        await asyncio.sleep(0.5)
        
        await test_client.stop()
        await calculator.stop()
    
    # Run the async test function
    asyncio.run(test_scenarios())


def run_gui() -> None:
    """Launch the Tkinter GUI."""
    gui = CalculatorGUI()
    gui.run()


if __name__ == "__main__":
    # Launch the GUI by default.
    if tk is None:
        print("Tkinter is not available. Falling back to the CLI demo.")
        asyncio.run(main())
    else:
        run_gui()