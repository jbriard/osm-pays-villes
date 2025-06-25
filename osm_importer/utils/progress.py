from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.console import Console
from typing import Dict, Any
import threading


class ProgressTracker:
    """Gestionnaire de barres de progression avec Rich."""

    def __init__(self):
        self.console = Console()
        self.progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed:,}/{task.total:,})"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console
        )
        self.tasks: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def add_task(self, name: str, description: str, total: int):
        """Ajoute une nouvelle tâche de progression."""
        with self._lock:
            task_id = self.progress.add_task(description, total=total)
            self.tasks[name] = task_id
        return task_id

    def update(self, name: str, advance: int = 1, completed: int = None, **kwargs):
        """Met à jour la progression d'une tâche."""
        with self._lock:
            if name in self.tasks:
                if completed is not None:
                    # Mettre à jour directement la valeur completed
                    current_task = self.progress.tasks[self.tasks[name]]
                    advance_value = completed - current_task.completed
                    self.progress.update(self.tasks[name], advance=advance_value, **kwargs)
                else:
                    self.progress.update(self.tasks[name], advance=advance, **kwargs)

    def update_total(self, name: str, new_total: int):
        """Met à jour le total d'une tâche."""
        with self._lock:
            if name in self.tasks:
                self.progress.update(self.tasks[name], total=new_total)

    def start(self):
        """Démarre l'affichage des barres de progression."""
        self.progress.start()

    def stop(self):
        """Arrête l'affichage des barres de progression."""
        self.progress.stop()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
