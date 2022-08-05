#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""Patches to apply to databases"""

from __future__ import annotations
from typing import *


PATCHES: list[tuple[str, str]] = [
    ('sql', '''
CREATE OR REPLACE FUNCTION
 edgedbstd."std|cast@std|json@array<std||json>_f"(val jsonb)
 RETURNS jsonb[]
 LANGUAGE sql
AS $function$
SELECT (
    CASE WHEN nullif(val, 'null'::jsonb) IS NULL THEN NULL
    ELSE
        (SELECT COALESCE(array_agg(j), ARRAY[]::jsonb[])
        FROM jsonb_array_elements(val) as j)
    END
)
$function$
    '''),
]
