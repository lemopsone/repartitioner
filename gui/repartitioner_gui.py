#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import time
from pathlib import Path
from tkinter import (
    BOTH,
    END,
    LEFT,
    RIGHT,
    VERTICAL,
    BooleanVar,
    StringVar,
    Text,
    Tk,
    filedialog,
    messagebox,
)
from tkinter import ttk


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_DIR = REPO_ROOT / ".gui_runs"


class RepartitionerGui:
    def __init__(self, root: Tk) -> None:
        self.root = root
        self.root.title("Repartitioner")
        self.root.geometry("1180x760")
        self.root.minsize(980, 640)

        self.process: subprocess.Popen[str] | None = None
        self.worker: threading.Thread | None = None
        self.events: queue.Queue[tuple[str, object]] = queue.Queue()

        self.input_path = StringVar()
        self.output_path = StringVar(value=str(REPO_ROOT / "data" / "gui_output_partitioned"))
        self.join_right_path = StringVar()
        self.key_columns = StringVar(value="user_id")
        self.job_type = StringVar(value="group_by")
        self.downstream_engine = StringVar(value="generic")
        self.input_format = StringVar(value="parquet")
        self.min_partitions = StringVar(value="16")
        self.max_partitions = StringVar(value="64")
        self.target_partition_size_mb = StringVar(value="128")
        self.target_file_size_mb = StringVar(value="128")
        self.min_file_size_mb = StringVar(value="16")
        self.heavy_key_alpha = StringVar(value="2.0")
        self.seed = StringVar(value="42")
        self.local_threads = StringVar(value=str(os.cpu_count() or 1))
        self.memory_limit_mb = StringVar(value="4096")
        self.no_op_max_imbalance_ratio = StringVar(value="1.2")
        self.approximate_capacity = StringVar(value="10000")
        self.broadcast_threshold_mb = StringVar(value="10")
        self.normal_key_assignment = StringVar(value="load_aware")
        self.heavy_hitter_mode = StringVar(value="exact")
        self.right_side_mode = StringVar(value="broadcast_if_small")
        self.force_rewrite = BooleanVar(value=False)
        self.include_technical_columns = BooleanVar(value=True)
        self.fail_on_memory_limit = BooleanVar(value=False)

        self.status = StringVar(value="Готово")
        self.metadata_choice = StringVar(value="_stats.json")

        self.build_layout()
        self.root.after(100, self.poll_events)

    def build_layout(self) -> None:
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=BOTH, expand=True, padx=8, pady=8)

        self.run_tab = ttk.Frame(self.notebook)
        self.results_tab = ttk.Frame(self.notebook)
        self.metadata_tab = ttk.Frame(self.notebook)
        self.log_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.run_tab, text="Запуск")
        self.notebook.add(self.results_tab, text="Результаты")
        self.notebook.add(self.metadata_tab, text="Метаданные")
        self.notebook.add(self.log_tab, text="Лог")

        self.build_run_tab()
        self.build_results_tab()
        self.build_metadata_tab()
        self.build_log_tab()

        bottom = ttk.Frame(self.root)
        bottom.pack(fill="x", padx=8, pady=(0, 8))
        self.progress = ttk.Progressbar(bottom, mode="indeterminate")
        self.progress.pack(side=LEFT, fill="x", expand=True)
        ttk.Label(bottom, textvariable=self.status, width=42, anchor="e").pack(side=RIGHT, padx=8)

    def build_run_tab(self) -> None:
        container = ttk.Frame(self.run_tab)
        container.pack(fill=BOTH, expand=True, padx=8, pady=8)

        dataset = ttk.LabelFrame(container, text="Данные")
        dataset.pack(fill="x", pady=(0, 8))
        self.add_path_row(
            dataset,
            0,
            "Входные данные",
            self.input_path,
            directory_command=lambda: self.choose_directory(self.input_path),
            file_command=lambda: self.choose_file(self.input_path),
        )
        self.add_path_row(
            dataset,
            1,
            "Выходная директория",
            self.output_path,
            directory_command=lambda: self.choose_directory(self.output_path),
            file_command=None,
        )
        ttk.Label(dataset, text="Формат входа").grid(row=2, column=0, sticky="w", padx=6, pady=4)
        ttk.Combobox(
            dataset,
            textvariable=self.input_format,
            values=("parquet", "csv"),
            state="readonly",
            width=16,
        ).grid(row=2, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(dataset, text="Ключевые столбцы").grid(row=2, column=2, sticky="w", padx=6, pady=4)
        ttk.Entry(dataset, textvariable=self.key_columns).grid(
            row=2, column=3, sticky="ew", padx=6, pady=4
        )
        dataset.columnconfigure(1, weight=1)
        dataset.columnconfigure(3, weight=1)

        job = ttk.LabelFrame(container, text="Назначение обработки")
        job.pack(fill="x", pady=(0, 8))
        ttk.Label(job, text="Тип последующей операции").grid(
            row=0, column=0, sticky="w", padx=6, pady=4
        )
        job_box = ttk.Combobox(
            job,
            textvariable=self.job_type,
            values=("group_by", "join", "filter", "scan", "generic"),
            state="readonly",
            width=18,
        )
        job_box.grid(row=0, column=1, sticky="w", padx=6, pady=4)
        job_box.bind("<<ComboboxSelected>>", lambda _event: self.update_join_controls())
        ttk.Label(job, text="Целевая система").grid(row=0, column=2, sticky="w", padx=6, pady=4)
        ttk.Combobox(
            job,
            textvariable=self.downstream_engine,
            values=("generic", "spark"),
            state="readonly",
            width=16,
        ).grid(row=0, column=3, sticky="w", padx=6, pady=4)
        self.join_frame = ttk.Frame(job)
        self.join_frame.grid(row=1, column=0, columnspan=4, sticky="ew")
        self.add_path_row(
            self.join_frame,
            0,
            "Правая таблица join",
            self.join_right_path,
            directory_command=lambda: self.choose_directory(self.join_right_path),
            file_command=lambda: self.choose_file(self.join_right_path),
        )
        ttk.Label(self.join_frame, text="Режим правой таблицы").grid(
            row=1, column=0, sticky="w", padx=6, pady=4
        )
        ttk.Combobox(
            self.join_frame,
            textvariable=self.right_side_mode,
            values=("broadcast_if_small", "shuffle"),
            state="readonly",
            width=18,
        ).grid(row=1, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(self.join_frame, text="Порог broadcast, МБ").grid(
            row=1, column=2, sticky="w", padx=6, pady=4
        )
        ttk.Entry(self.join_frame, textvariable=self.broadcast_threshold_mb, width=10).grid(
            row=1, column=3, sticky="w", padx=6, pady=4
        )
        self.join_frame.columnconfigure(1, weight=1)
        self.update_join_controls()

        params = ttk.LabelFrame(container, text="Параметры метода")
        params.pack(fill="x", pady=(0, 8))
        self.add_labeled_entry(params, 0, 0, "Мин. число партиций", self.min_partitions)
        self.add_labeled_entry(params, 0, 2, "Макс. число партиций", self.max_partitions)
        self.add_labeled_entry(
            params, 0, 4, "Целевой размер партиции, МБ", self.target_partition_size_mb
        )
        self.add_labeled_entry(params, 1, 0, "Порог тяжёлого ключа", self.heavy_key_alpha)
        self.add_labeled_entry(params, 1, 2, "Начальное значение", self.seed)
        self.add_labeled_entry(
            params, 1, 4, "Порог пропуска обработки", self.no_op_max_imbalance_ratio
        )
        self.add_labeled_entry(params, 2, 0, "Локальные потоки", self.local_threads)
        self.add_labeled_entry(params, 2, 2, "Лимит памяти, МБ", self.memory_limit_mb)

        buttons = ttk.Frame(container)
        buttons.pack(fill="x", pady=8)
        self.run_button = ttk.Button(buttons, text="Запустить обработку", command=self.run)
        self.run_button.pack(side=LEFT)
        self.cancel_button = ttk.Button(
            buttons, text="Остановить", command=self.cancel, state="disabled"
        )
        self.cancel_button.pack(side=LEFT, padx=8)
        ttk.Button(buttons, text="Обновить результаты", command=self.load_results).pack(side=LEFT)

    def build_results_tab(self) -> None:
        top = ttk.Frame(self.results_tab)
        top.pack(fill="x", padx=8, pady=8)
        self.summary_labels: dict[str, StringVar] = {}
        summary_items = [
            ("rows", "Строк"),
            ("distinct_keys", "Ключей"),
            ("heavy_hitter_count", "Тяжёлых ключей"),
            ("output_partitions", "Партиций"),
            ("output_files", "Файлов"),
            ("rewrite_required", "Перезапись"),
            ("before_rho", "ρ до"),
            ("after_rho", "ρ после"),
            ("total_seconds", "Время, с"),
        ]
        for index, (key, label) in enumerate(summary_items):
            row = index // 4
            col = (index % 4) * 2
            ttk.Label(top, text=label).grid(row=row, column=col, sticky="w", padx=6, pady=4)
            var = StringVar(value="-")
            self.summary_labels[key] = var
            ttk.Label(top, textvariable=var, font=("", 10, "bold")).grid(
                row=row, column=col + 1, sticky="w", padx=6, pady=4
            )

        middle = ttk.PanedWindow(self.results_tab, orient=VERTICAL)
        middle.pack(fill=BOTH, expand=True, padx=8, pady=(0, 8))

        files_frame = ttk.LabelFrame(middle, text="Выходные партиции")
        middle.add(files_frame, weight=3)
        columns = ("partition", "rows", "files", "size", "path")
        self.files_tree = ttk.Treeview(files_frame, columns=columns, show="headings")
        headings = {
            "partition": "Партиция",
            "rows": "Строк",
            "files": "Файлов",
            "size": "Размер, байт",
            "path": "Файлы",
        }
        widths = {"partition": 90, "rows": 120, "files": 80, "size": 120, "path": 560}
        for column in columns:
            self.files_tree.heading(column, text=headings[column])
            self.files_tree.column(column, width=widths[column], anchor="w")
        yscroll = ttk.Scrollbar(files_frame, orient=VERTICAL, command=self.files_tree.yview)
        self.files_tree.configure(yscrollcommand=yscroll.set)
        self.files_tree.pack(side=LEFT, fill=BOTH, expand=True)
        yscroll.pack(side=RIGHT, fill="y")

        stats_frame = ttk.LabelFrame(middle, text="Сводка")
        middle.add(stats_frame, weight=2)
        self.stats_text = Text(stats_frame, wrap="word", height=10)
        self.stats_text.pack(fill=BOTH, expand=True)

    def build_metadata_tab(self) -> None:
        controls = ttk.Frame(self.metadata_tab)
        controls.pack(fill="x", padx=8, pady=8)
        ttk.Label(controls, text="Файл").pack(side=LEFT)
        ttk.Combobox(
            controls,
            textvariable=self.metadata_choice,
            values=("_stats.json", "_partition_plan.json", "_manifest.json", "_gui_config.yaml"),
            state="readonly",
            width=24,
        ).pack(side=LEFT, padx=8)
        ttk.Button(controls, text="Показать", command=self.show_selected_metadata).pack(side=LEFT)

        self.metadata_text = Text(self.metadata_tab, wrap="none")
        self.metadata_text.pack(fill=BOTH, expand=True, padx=8, pady=(0, 8))

    def build_log_tab(self) -> None:
        self.log_text = Text(self.log_tab, wrap="word")
        self.log_text.pack(fill=BOTH, expand=True, padx=8, pady=8)

    def add_path_row(
        self,
        parent: ttk.Frame,
        row: int,
        label: str,
        variable: StringVar,
        *,
        directory_command,
        file_command,
    ) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=6, pady=4)
        ttk.Entry(parent, textvariable=variable).grid(
            row=row, column=1, columnspan=2, sticky="ew", padx=6, pady=4
        )
        ttk.Button(parent, text="Каталог", command=directory_command).grid(
            row=row, column=3, sticky="ew", padx=6, pady=4
        )
        if file_command is not None:
            ttk.Button(parent, text="Файл", command=file_command).grid(
                row=row, column=4, sticky="ew", padx=6, pady=4
            )

    def add_labeled_entry(
        self, parent: ttk.Frame, row: int, column: int, label: str, variable: StringVar
    ) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=column, sticky="w", padx=6, pady=4)
        ttk.Entry(parent, textvariable=variable, width=12).grid(
            row=row, column=column + 1, sticky="w", padx=6, pady=4
        )

    def choose_directory(self, variable: StringVar) -> None:
        selected = filedialog.askdirectory()
        if selected:
            variable.set(selected)

    def choose_file(self, variable: StringVar) -> None:
        selected = filedialog.askopenfilename(
            filetypes=(("Datasets", "*.parquet *.csv"), ("All files", "*.*"))
        )
        if selected:
            variable.set(selected)

    def update_join_controls(self) -> None:
        state = "normal" if self.job_type.get() == "join" else "disabled"
        for child in self.join_frame.winfo_children():
            try:
                child.configure(state=state)
            except Exception:
                pass

    def run(self) -> None:
        try:
            config = self.build_config()
        except ValueError as error:
            messagebox.showerror("Ошибка конфигурации", str(error))
            return

        RUN_DIR.mkdir(parents=True, exist_ok=True)
        config_path = RUN_DIR / f"gui-config-{int(time.time())}.yaml"
        config_path.write_text(config, encoding="utf-8")
        (Path(self.output_path.get()) / "_gui_config.yaml").parent.mkdir(parents=True, exist_ok=True)

        command = self.resolve_command() + ["--config", str(config_path)]
        self.set_running(True)
        self.clear_log()
        self.append_log("+ " + " ".join(command) + "\n")
        self.append_log(f"config: {config_path}\n\n")
        self.status.set("Обработка выполняется")
        self.worker = threading.Thread(
            target=self.run_worker,
            args=(command, config_path),
            daemon=True,
        )
        self.worker.start()

    def run_worker(self, command: list[str], config_path: Path) -> None:
        try:
            self.process = subprocess.Popen(
                command,
                cwd=REPO_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert self.process.stdout is not None
            for line in self.process.stdout:
                self.events.put(("log", line))
            return_code = self.process.wait()
            if return_code == 0:
                output_config_path = Path(self.output_path.get()) / "_gui_config.yaml"
                output_config_path.write_text(config_path.read_text(encoding="utf-8"), encoding="utf-8")
                self.events.put(("done", return_code))
            else:
                self.events.put(("failed", return_code))
        except Exception as error:
            self.events.put(("error", str(error)))
        finally:
            self.process = None

    def cancel(self) -> None:
        if self.process is not None:
            self.append_log("\nОстановка процесса...\n")
            self.process.terminate()

    def poll_events(self) -> None:
        try:
            while True:
                kind, payload = self.events.get_nowait()
                if kind == "log":
                    self.append_log(str(payload))
                elif kind == "done":
                    self.set_running(False)
                    self.status.set("Обработка завершена")
                    self.append_log("\nГотово.\n")
                    self.load_results()
                    self.notebook.select(self.results_tab)
                elif kind == "failed":
                    self.set_running(False)
                    self.status.set(f"Ошибка выполнения: код {payload}")
                    self.append_log(f"\nПроцесс завершился с кодом {payload}.\n")
                    self.notebook.select(self.log_tab)
                elif kind == "error":
                    self.set_running(False)
                    self.status.set("Ошибка")
                    self.append_log(f"\nОшибка GUI: {payload}\n")
                    self.notebook.select(self.log_tab)
        except queue.Empty:
            pass
        self.root.after(100, self.poll_events)

    def set_running(self, running: bool) -> None:
        self.run_button.configure(state="disabled" if running else "normal")
        self.cancel_button.configure(state="normal" if running else "disabled")
        if running:
            self.progress.start(10)
        else:
            self.progress.stop()

    def build_config(self) -> str:
        input_path = Path(self.input_path.get()).expanduser()
        output_path = Path(self.output_path.get()).expanduser()
        if not self.input_path.get().strip():
            raise ValueError("Выберите входной dataset.")
        if not input_path.exists():
            raise ValueError(f"Входной путь не существует: {input_path}")
        if not self.output_path.get().strip():
            raise ValueError("Выберите выходную директорию.")
        key_columns = [item.strip() for item in self.key_columns.get().split(",") if item.strip()]
        if not key_columns:
            raise ValueError("Укажите хотя бы одну ключевую колонку.")

        min_partitions = self.positive_int("Мин. число партиций", self.min_partitions)
        max_partitions = self.positive_int("Макс. число партиций", self.max_partitions)
        if min_partitions > max_partitions:
            raise ValueError("Мин. число партиций не может быть больше макс. числа партиций.")

        sections = [
            "dataset:",
            f"  input: {yaml_string(input_path)}",
            f"  input_format: {yaml_string(self.input_format.get())}",
            "",
            "output:",
            f"  path: {yaml_string(output_path)}",
            '  format: "parquet"',
            f"  include_technical_columns: {yaml_bool(self.include_technical_columns.get())}",
            '  partition_column: "_rp_partition_id"',
            '  salt_column: "_rp_salt"',
            '  heavy_key_column: "_rp_is_heavy_key"',
            "",
            "partitioning:",
            f"  key_columns: {yaml_list(key_columns)}",
            f"  target_partition_size_mb: {self.positive_int('Целевой размер партиции', self.target_partition_size_mb)}",
            f"  min_partitions: {min_partitions}",
            f"  max_partitions: {max_partitions}",
            '  strategy: "adaptive_hash_salt"',
            f"  normal_key_assignment: {yaml_string(self.normal_key_assignment.get())}",
            f"  heavy_key_alpha: {self.positive_float('Порог тяжёлого ключа', self.heavy_key_alpha)}",
            f"  force_rewrite: {yaml_bool(self.force_rewrite.get())}",
            f"  no_op_max_imbalance_ratio: {self.positive_float('Порог пропуска обработки', self.no_op_max_imbalance_ratio)}",
            f"  seed: {self.non_negative_int('Seed', self.seed)}",
            "",
            "statistics:",
            f"  heavy_hitter_mode: {yaml_string(self.heavy_hitter_mode.get())}",
            f"  approximate_capacity: {self.positive_int('Ёмкость приближённого подсчёта', self.approximate_capacity)}",
            "",
            "storage:",
            f"  target_file_size_mb: {self.positive_int('Целевой размер файла', self.target_file_size_mb)}",
            f"  min_file_size_mb: {self.positive_int('Минимальный размер файла', self.min_file_size_mb)}",
            "",
            "job:",
            f"  type: {yaml_string(self.job_type.get())}",
            f"  downstream_engine: {yaml_string(self.downstream_engine.get())}",
        ]
        if self.job_type.get() == "join":
            right = Path(self.join_right_path.get()).expanduser()
            if not self.join_right_path.get().strip():
                raise ValueError("Для job type = join выберите правую сторону join.")
            if not right.exists():
                raise ValueError(f"Правая сторона join не существует: {right}")
            sections.extend(
                [
                    "",
                    "join:",
                    f"  left_input: {yaml_string(input_path)}",
                    f"  right_input: {yaml_string(right)}",
                    f"  join_keys: {yaml_list(key_columns)}",
                    f"  right_side_mode: {yaml_string(self.right_side_mode.get())}",
                    f"  broadcast_threshold_mb: {self.positive_int('Порог broadcast', self.broadcast_threshold_mb)}",
                ]
            )
        sections.extend(
            [
                "",
                "resources:",
                f"  local_threads: {self.positive_int('Локальные потоки', self.local_threads)}",
                f"  memory_limit_mb: {self.positive_int('Лимит памяти', self.memory_limit_mb)}",
                f"  fail_on_memory_limit: {yaml_bool(self.fail_on_memory_limit.get())}",
                "",
            ]
        )
        return "\n".join(sections)

    def resolve_command(self) -> list[str]:
        release_binary = REPO_ROOT / "target" / "release" / "repartitioner"
        debug_binary = REPO_ROOT / "target" / "debug" / "repartitioner"
        if release_binary.exists():
            return [str(release_binary)]
        if debug_binary.exists():
            return [str(debug_binary)]
        return ["cargo", "run", "-p", "repartitioner", "--"]

    def load_results(self) -> None:
        output_dir = Path(self.output_path.get())
        try:
            plan = read_json(output_dir / "_partition_plan.json")
            stats = read_json(output_dir / "_stats.json")
            manifest = read_json(output_dir / "_manifest.json")
        except Exception as error:
            messagebox.showwarning("Результаты не найдены", str(error))
            return

        input_stats = stats.get("input", {})
        before = stats.get("before_skew") or {}
        after = stats.get("after_skew") or {}
        timing = stats.get("timing") or {}
        self.summary_labels["rows"].set(format_number(input_stats.get("total_rows")))
        self.summary_labels["distinct_keys"].set(format_number(input_stats.get("distinct_keys")))
        self.summary_labels["heavy_hitter_count"].set(
            str(len(input_stats.get("heavy_hitters") or []))
        )
        self.summary_labels["output_partitions"].set(str(plan.get("output_partitions", "-")))
        self.summary_labels["output_files"].set(str(len(manifest.get("output_files") or [])))
        self.summary_labels["rewrite_required"].set("Да" if plan.get("rewrite_required") else "Нет")
        self.summary_labels["before_rho"].set(format_float(before.get("max_mean_imbalance_ratio")))
        self.summary_labels["after_rho"].set(format_float(after.get("max_mean_imbalance_ratio")))
        self.summary_labels["total_seconds"].set(format_float(timing.get("total_seconds")))

        self.populate_files(manifest)
        self.populate_stats(plan, stats, manifest)
        self.show_selected_metadata()

    def populate_files(self, manifest: dict) -> None:
        self.files_tree.delete(*self.files_tree.get_children())
        output_files = manifest.get("output_files") or []
        files_by_partition: dict[object, list[dict]] = {}
        for file_info in output_files:
            partition_id = file_info.get("partition_id")
            files_by_partition.setdefault(partition_id, []).append(file_info)

        partitions = manifest.get("partitions") or []
        if partitions:
            for partition in partitions:
                partition_id = partition.get("partition_id")
                partition_files = files_by_partition.get(partition_id, [])
                size_bytes = partition.get("size_bytes")
                if size_bytes is None:
                    size_bytes = sum_file_sizes(partition_files)
                paths = "; ".join(
                    file_info.get("path", "") for file_info in partition_files if file_info.get("path")
                )
                self.files_tree.insert(
                    "",
                    END,
                    values=(
                        partition_id,
                        format_number(partition.get("row_count")),
                        partition.get("file_count", len(partition_files)),
                        format_number(size_bytes),
                        paths or "-",
                    ),
                )
            return

        for partition_id, partition_files in sorted(files_by_partition.items(), key=lambda item: str(item[0])):
            paths = "; ".join(
                file_info.get("path", "") for file_info in partition_files if file_info.get("path")
            )
            self.files_tree.insert(
                "",
                END,
                values=(
                    partition_id,
                    format_number(sum((file_info.get("row_count") or 0) for file_info in partition_files)),
                    len(partition_files),
                    format_number(sum_file_sizes(partition_files)),
                    paths or "-",
                ),
            )

    def populate_stats(self, plan: dict, stats: dict, manifest: dict) -> None:
        input_stats = stats.get("input", {})
        before = stats.get("before_skew") or {}
        after = stats.get("after_skew") or {}
        timing = stats.get("timing") or {}
        heavy_hitters = input_stats.get("heavy_hitters") or []
        lines = [
            "Разбиение:",
            f"  до обработки: ρ = {format_float(before.get('max_mean_imbalance_ratio'))}, "
            + f"макс. = {format_number(before.get('max_partition_size'))}, "
            + f"p95 = {format_float(before.get('p95_partition_size'))}, "
            + f"CV = {format_float(before.get('coefficient_of_variation'))}",
            f"  после обработки: ρ = {format_float(after.get('max_mean_imbalance_ratio'))}, "
            + f"макс. = {format_number(after.get('max_partition_size'))}, "
            + f"p95 = {format_float(after.get('p95_partition_size'))}, "
            + f"CV = {format_float(after.get('coefficient_of_variation'))}",
            "",
            "Ключи:",
            f"  уникальных: {format_number(input_stats.get('distinct_keys'))}",
            f"  максимальная частота: {format_number(input_stats.get('max_key_frequency'))}",
            f"  тяжёлых ключей: {len(heavy_hitters)}",
            "",
            "Время:",
            f"  статистика: {format_float(timing.get('statistics_seconds'))} с",
            f"  планирование: {format_float(timing.get('planning_seconds'))} с",
            f"  запись: {format_float(timing.get('writing_seconds'))} с",
            f"  всего: {format_float(timing.get('total_seconds'))} с",
        ]
        if heavy_hitters:
            lines.extend(["", "Первые тяжёлые ключи:"])
            for item in heavy_hitters[:5]:
                lines.append(
                    "  "
                    + f"{item.get('key')}: частота = {format_number(item.get('estimated_frequency'))}, "
                    + f"солей = {item.get('salt_count')}"
                )
            if len(heavy_hitters) > 5:
                lines.append(f"  ... ещё {len(heavy_hitters) - 5}")
        self.stats_text.delete("1.0", END)
        self.stats_text.insert("1.0", "\n".join(lines))

    def show_selected_metadata(self) -> None:
        output_dir = Path(self.output_path.get())
        path = output_dir / self.metadata_choice.get()
        self.metadata_text.delete("1.0", END)
        if not path.exists():
            self.metadata_text.insert("1.0", f"Файл не найден: {path}")
            return
        if path.suffix == ".json":
            payload = json.dumps(read_json(path), ensure_ascii=False, indent=2)
        else:
            payload = path.read_text(encoding="utf-8")
        self.metadata_text.insert("1.0", payload)

    def clear_log(self) -> None:
        self.log_text.delete("1.0", END)

    def append_log(self, text: str) -> None:
        self.log_text.insert(END, text)
        self.log_text.see(END)

    def positive_int(self, label: str, variable: StringVar) -> int:
        try:
            value = int(variable.get())
        except ValueError as exc:
            raise ValueError(f"{label}: требуется целое число.") from exc
        if value <= 0:
            raise ValueError(f"{label}: значение должно быть больше нуля.")
        return value

    def non_negative_int(self, label: str, variable: StringVar) -> int:
        try:
            value = int(variable.get())
        except ValueError as exc:
            raise ValueError(f"{label}: требуется целое число.") from exc
        if value < 0:
            raise ValueError(f"{label}: значение не должно быть отрицательным.")
        return value

    def positive_float(self, label: str, variable: StringVar) -> float:
        try:
            value = float(variable.get())
        except ValueError as exc:
            raise ValueError(f"{label}: требуется число.") from exc
        if value <= 0:
            raise ValueError(f"{label}: значение должно быть больше нуля.")
        return value


def yaml_string(value) -> str:
    return json.dumps(str(value), ensure_ascii=False)


def yaml_list(values: list[str]) -> str:
    return "[" + ", ".join(yaml_string(value) for value in values) + "]"


def yaml_bool(value: bool) -> str:
    return "true" if value else "false"


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def sum_file_sizes(files: list[dict]):
    sizes = [file_info.get("size_bytes") for file_info in files]
    if not sizes or any(size is None for size in sizes):
        return None
    return sum(sizes)


def format_float(value) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return str(value)


def format_number(value) -> str:
    if value is None:
        return "-"
    try:
        return f"{int(value):,}".replace(",", " ")
    except (TypeError, ValueError):
        return str(value)


def main() -> None:
    root = Tk()
    RepartitionerGui(root)
    root.mainloop()


if __name__ == "__main__":
    main()
