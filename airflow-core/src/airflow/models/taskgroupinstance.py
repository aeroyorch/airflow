# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Persist retry state for TaskGroups within a DagRun."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Index, Integer
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime


class TaskGroupInstance(Base):
    """Persist retry state for a TaskGroup within a DagRun."""

    __tablename__ = "task_group_instance"

    dag_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    run_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    task_group_id: Mapped[str] = mapped_column(StringID(), primary_key=True)

    try_number: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    next_retry_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, nullable=False, default=timezone.utcnow, onupdate=timezone.utcnow
    )

    __table_args__ = (Index("idx_task_group_instance_dag_run", "dag_id", "run_id"),)

    def __repr__(self) -> str:
        return (
            "<TaskGroupInstance "
            f"{self.dag_id}.{self.run_id}.{self.task_group_id} "
            f"try_number={self.try_number} next_retry_at={self.next_retry_at}>"
        )
