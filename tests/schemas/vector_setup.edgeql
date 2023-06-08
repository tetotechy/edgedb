#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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


create extension vector;


create scalar type v3 extending vector::vector<3>;

create scalar type myf64 extending float64 {
    create constraint max_value(100);
};
create scalar type deepf64 extending myf64;


create type Basic {
    create required property p_str -> str;
    create property p_json -> json {
        create rewrite insert using (to_json(__subject__.p_str));
    };
};

create type L2 {
    create required property vec -> v3;
    create index vector::ivfflat_euclidean(lists := 100) on (.vec);
};

create type IP {
    create required property vec -> v3;
    create index vector::ivfflat_ip(lists := 100) on (.vec);
};

create type Cosine {
    create required property vec -> v3;
    create index vector::ivfflat_cosine(lists := 100) on (.vec);
};

create type Con {
    create required property vec -> v3 {
        create constraint expression on (
            vector::cosine_distance(
                __subject__, <vector::vector>[1, 1, 1]
            ) < 0.2
        );
    };
};


create type Raw {
    create required property val -> float64;

    create property p_int16 -> int16 {
        create rewrite insert using (<int16>__subject__.val)
    };
    create property p_int32 -> int32 {
        create rewrite insert using (<int32>__subject__.val)
    };
    create property p_int64 -> int64 {
        create rewrite insert using (<int64>__subject__.val)
    };
    create property p_bigint -> bigint {
        create rewrite insert using (<bigint>__subject__.val)
    };
    create property p_float32 -> float32 {
        create rewrite insert using (<float32>__subject__.val)
    };
    create property p_decimal -> decimal {
        create rewrite insert using (<decimal>__subject__.val)
    };

    create property p_myf64 -> myf64 {
        create rewrite insert using (<myf64>__subject__.val)
    };
    create property p_deepf64 -> deepf64 {
        create rewrite insert using (<deepf64>__subject__.val)
    };
};


for x in {0, 3, 4.25, 6.75}
union (
    insert Raw {val := x}
);


for x in {'[0, 1, 2.3]', '[1, 1, 10.11]', '[4.5, 6.7, 8.9]'}
union (
    (insert Basic {p_str := x}),
    (insert L2 {vec := <v3>x}),
    (insert IP {vec := <v3>x}),
    (insert Cosine {vec := <v3>x}),
);
