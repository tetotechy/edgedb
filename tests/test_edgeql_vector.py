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

import os

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class RollBack(Exception):
    pass


class TestEdgeQLVector(tb.QueryTestCase):
    EXTENSIONS = ['vector']

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'vector.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'vector_setup.edgeql')

    async def test_edgeql_vector_cast_01(self):
        # Basic casts to and from str and json. Also a cast into an
        # array<float32>.
        await self.assert_query_result(
            '''
                select <str><vector::vector>'[1, 2, 3.5]';
            ''',
            ['[1,2,3.5]'],
        )

        await self.assert_query_result(
            '''
                select <json><vector::vector>'[1, 2, 3.5]';
            ''',
            [[1, 2, 3.5]],
            json_only=True,
        )

        await self.assert_query_result(
            '''
                select <str><vector::vector><json>[1, 2, 3.5];
            ''',
            ['[1,2,3.5]'],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><vector::vector>'[1.5, 2, 3]';
            ''',
            [[1.5, 2, 3]],
        )

    async def test_edgeql_vector_cast_02(self):
        # Basic casts from str and json.
        await self.assert_query_result(
            '''
                select <array<float32>><vector::vector>(
                    select _ := Basic.p_str order by _
                )
            ''',
            [[0, 1, 2.3], [1, 1, 10.11], [4.5, 6.7, 8.9]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><vector::vector>(
                    select _ := Basic.p_json order by _
                )
            ''',
            [[0, 1, 2.3], [1, 1, 10.11], [4.5, 6.7, 8.9]],
        )

    async def test_edgeql_vector_cast_03(self):
        # Casts from numeric array expressions.
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector><array<int16>>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector><array<int32>>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector><array<int64>>[1.0, 2.0, 3.0];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector><array<float32>>[1.5, 2, 3];
            ''',
            [[1.5, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector><array<float64>>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector><array<decimal>>[1.5, 2, 3];
            ''',
            [[1.5, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector><array<bigint>>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

    async def test_edgeql_vector_cast_04(self):
        # Casts from numeric array expressions.
        res = [0, 3, 4, 7]
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(
                        Raw.p_int16 order by Raw.p_int16
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(
                        Raw.p_int32 order by Raw.p_int32
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(
                        Raw.p_int64 order by Raw.p_int64
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(
                        Raw.p_bigint order by Raw.p_bigint
                    );
            ''',
            [res],
        )

    async def test_edgeql_vector_cast_05(self):
        # Casts from numeric array expressions.
        res = [0, 3, 4.25, 6.75]
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(
                        Raw.p_float32 order by Raw.p_float32
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(
                        Raw.val order by Raw.val
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(
                        Raw.p_decimal order by Raw.p_decimal
                    );
            ''',
            [res],
        )

    async def test_edgeql_vector_cast_06(self):
        # Casts from literal numeric arrays.
        await self.assert_query_result(
            '''
                select <array<float32>><vector::vector>[1, 2, 3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><vector::vector>
                    [<int16>1, <int16>2, <int16>3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><vector::vector>
                    [<int32>1, <int32>2, <int32>3];
            ''',
            [[1, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><vector::vector>[1.5, 2, 3];
            ''',
            [[1.5, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><vector::vector>
                    [<float32>1.5, <float32>2, <float32>3];
            ''',
            [[1.5, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><vector::vector>[1.5n, 2n, 3n];
            ''',
            [[1.5, 2, 3]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>><vector::vector>[1n, 2n, 3n];
            ''',
            [[1, 2, 3]],
        )

    async def test_edgeql_vector_cast_07(self):
        await self.assert_query_result(
            '''
                select <array<float32>><v3>[11, 3, 4];
            ''',
            [[11, 3, 4]],
        )

    async def test_edgeql_vector_cast_08(self):
        # Casts from arrays of derived types.
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector><array<myf64>>[1, 2.3, 4.5];
            ''',
            [[1, 2.3, 4.5]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector><array<deepf64>>[1, 2.3, 4.5];
            ''',
            [[1, 2.3, 4.5]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(<myf64>{1, 2.3, 4.5});
            ''',
            [[1, 2.3, 4.5]],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(<deepf64>{1, 2.3, 4.5});
            ''',
            [[1, 2.3, 4.5]],
        )

    async def test_edgeql_vector_cast_09(self):
        # Casts from arrays of derived types.
        res = [0, 3, 4.25, 6.75]
        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(
                        Raw.p_myf64 order by Raw.p_myf64
                    );
            ''',
            [res],
        )

        await self.assert_query_result(
            '''
                select <array<float32>>
                    <vector::vector>array_agg(
                        Raw.p_deepf64 order by Raw.p_deepf64
                    );
            ''',
            [res],
        )

    @test.xfail('vectors appear as JSON strings when nested')
    async def test_edgeql_vector_cast_10(self):
        # Arrays of vectors.
        await self.assert_query_result(
            '''
                with module vector
                select [
                    <vector>[0, 1],
                    <vector>[2, 3],
                    <vector>[4, 5, 6],
                ]
            ''',
            [[[0, 1], [2, 3], [4, 5, 6]]],
            json_only=True,
        )

    async def test_edgeql_vector_cast_11(self):
        # Vectors in tuples.
        await self.assert_query_result(
            '''
                with module vector
                select (
                    <vector>[0, 1],
                    <vector>[2, 3],
                    <vector>[4, 5, 6],
                )
            ''',
            [[[0, 1], [2, 3], [4, 5, 6]]],
            json_only=True,
        )

    async def test_edgeql_vector_op_01(self):
        # Comparison operators.
        await self.assert_query_result(
            '''
                select <vector::vector>'[1, 2, 3]' =
                    <vector::vector>'[0, 1, 1]';
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                select <vector::vector>'[1, 2, 3]' !=
                    <vector::vector>'[0, 1, 1]';
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <vector::vector>'[1, 2, 3]' ?=
                    <vector::vector>{};
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                select <vector::vector>'[1, 2, 3]' ?!=
                    <vector::vector>{};
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <vector::vector>{} ?=
                    <vector::vector>{};
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <vector::vector>'[1, 2]' <
                    <vector::vector>'[2, 3]';
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <vector::vector>'[1, 2]' <=
                    <vector::vector>'[2, 3]';
            ''',
            [True],
        )

        await self.assert_query_result(
            '''
                select <vector::vector>'[1, 2]' >
                    <vector::vector>'[2, 3]';
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                select <vector::vector>'[1, 2]' >=
                    <vector::vector>'[2, 3]';
            ''',
            [False],
        )

    async def test_edgeql_vector_op_02(self):
        await self.assert_query_result(
            '''
                with module vector
                select <vector>'[3, 0]' in {
                    <vector>'[1, 2]',
                    <vector>'[3, 4]',
                };
            ''',
            [False],
        )

        await self.assert_query_result(
            '''
                with module vector
                select <vector>'[3, 0]' not in {
                    <vector>'[1, 2]',
                    <vector>'[3, 4]',
                };
            ''',
            [True],
        )

    async def test_edgeql_vector_func_01(self):
        await self.assert_query_result(
            '''
                with module vector
                select len(
                    <vector>'[1.2, 3.4, 5, 6]',
                );
            ''',
            [4],
        )

        await self.assert_query_result(
            '''
                with module vector
                select len(default::L2.vec) limit 1;
            ''',
            [3],
        )

    async def test_edgeql_vector_func_02(self):
        await self.assert_query_result(
            '''
                with module vector
                select euclidean_distance(
                    <vector>[3, 4],
                    <vector>[0, 0],
                );
            ''',
            [5],
        )

        await self.assert_query_result(
            '''
                with module vector
                select euclidean_distance(
                    default::L2.vec,
                    <vector>[0, 1, 0],
                );
            ''',
            {2.299999952316284, 10.159335266542493, 11.48694872607437},
        )

    async def test_edgeql_vector_func_03(self):
        await self.assert_query_result(
            '''
                with module vector
                select euclidean_norm(<vector>'[3, 4]');
            ''',
            [5],
        )

        await self.assert_query_result(
            '''
                with module vector
                select euclidean_norm(default::L2.vec);
            ''',
            {2.5079872331917934, 10.208432276239787, 12.014573942925704},
        )

    async def test_edgeql_vector_func_04(self):
        await self.assert_query_result(
            '''
                with module vector
                select inner_product(
                    <vector>[1, 2],
                    <vector>[3, 4],
                );
            ''',
            [11],
        )

        await self.assert_query_result(
            '''
                with module vector
                select inner_product(
                    default::IP.vec,
                    <vector>[3, 4, 1],
                );
            ''',
            {6.299999952316284, 17.109999656677246, 49.19999885559082},
        )

    async def test_edgeql_vector_func_05(self):
        await self.assert_query_result(
            '''
                with module vector
                select cosine_distance(
                    <vector>[3, 0],
                    <vector>[3, 4],
                );
            ''',
            [0.4],
        )

        await self.assert_query_result(
            '''
                with module vector
                select cosine_distance(
                    default::Cosine.vec,
                    <vector>[3, 4, 1],
                );
            ''',
            {0.5073612713543951, 0.6712965405380352, 0.19689922670600213},
        )

    async def test_edgeql_vector_func_06(self):
        await self.assert_query_result(
            '''
                with module vector
                select <array<float32>>
                    mean({
                        <vector>[3, 0],
                        <vector>[0, 4],
                    });
            ''',
            [[1.5, 2]],
        )

        await self.assert_query_result(
            '''
                with module vector
                select <array<float32>>
                    mean(default::L2.vec);
            ''',
            [[1.8333334, 2.8999999, 7.103333]],
        )

    async def test_edgeql_vector_insert_01(self):
        # Test assignment casts
        res = [0, 3, 4]
        with self.assertRaises(RollBack):
            async with self.con.transaction():
                await self.con.execute(r"""
                    insert L2 {
                        vec := array_agg(
                            Raw.p_int16 order by Raw.p_int16
                        )[:3]
                    }
                """)
                await self.assert_query_result(
                    '''
                        with res := <array<float32>>$res
                        select <array<float32>>(
                            select L2
                            filter .vec = <vector::vector>res
                        ).vec
                    ''',
                    [res],
                    variables=dict(res=res),
                )
                raise RollBack

        with self.assertRaises(RollBack):
            async with self.con.transaction():
                await self.con.execute(r"""
                    insert L2 {
                        vec := array_agg(
                            Raw.p_int32 order by Raw.p_int32
                        )[:3]
                    }
                """)
                await self.assert_query_result(
                    '''
                        with res := <array<float32>>$res
                        select <array<float32>>(
                            select L2
                            filter .vec = <vector::vector>res
                        ).vec
                    ''',
                    [res],
                    variables=dict(res=res),
                )
                raise RollBack

        with self.assertRaises(RollBack):
            async with self.con.transaction():
                await self.con.execute(r"""
                    insert L2 {
                        vec := array_agg(
                            Raw.p_int64 order by Raw.p_int64
                        )[:3]
                    }
                """)
                await self.assert_query_result(
                    '''
                        with res := <array<float32>>$res
                        select <array<float32>>(
                            select L2
                            filter .vec = <vector::vector>res
                        ).vec
                    ''',
                    [res],
                    variables=dict(res=res),
                )
                raise RollBack

        with self.assertRaises(RollBack):
            async with self.con.transaction():
                await self.con.execute(r"""
                    insert L2 {
                        vec := array_agg(
                            Raw.p_bigint order by Raw.p_bigint
                        )[:3]
                    }
                """)
                await self.assert_query_result(
                    '''
                        with res := <array<float32>>$res
                        select <array<float32>>(
                            select L2
                            filter .vec = <vector::vector>res
                        ).vec
                    ''',
                    [res],
                    variables=dict(res=res),
                )
                raise RollBack

    async def test_edgeql_vector_insert_02(self):
        # Test assignment casts
        res = [0, 3, 4.25]
        with self.assertRaises(RollBack):
            async with self.con.transaction():
                await self.con.execute(r"""
                    insert L2 {
                        vec := array_agg(
                            Raw.p_float32 order by Raw.p_float32
                        )[:3]
                    }
                """)
                await self.assert_query_result(
                    '''
                        with res := <array<float32>>$res
                        select <array<float32>>(
                            select L2
                            filter .vec = <vector::vector>res
                        ).vec
                    ''',
                    [res],
                    variables=dict(res=res),
                )
                raise RollBack

        with self.assertRaises(RollBack):
            async with self.con.transaction():
                await self.con.execute(r"""
                    insert L2 {
                        vec := array_agg(
                            Raw.val order by Raw.val
                        )[:3]
                    }
                """)
                await self.assert_query_result(
                    '''
                        with res := <array<float32>>$res
                        select <array<float32>>(
                            select L2
                            filter .vec = <vector::vector>res
                        ).vec
                    ''',
                    [res],
                    variables=dict(res=res),
                )
                raise RollBack

        with self.assertRaises(RollBack):
            async with self.con.transaction():
                await self.con.execute(r"""
                    insert L2 {
                        vec := array_agg(
                            Raw.p_decimal order by Raw.p_decimal
                        )[:3]
                    }
                """)
                await self.assert_query_result(
                    '''
                        with res := <array<float32>>$res
                        select <array<float32>>(
                            select L2
                            filter .vec = <vector::vector>res
                        ).vec
                    ''',
                    [res],
                    variables=dict(res=res),
                )
                raise RollBack

    async def test_edgeql_vector_constraint_01(self):
        with self.assertRaises(RollBack):
            async with self.con.transaction():
                await self.con.execute(r"""
                    insert Con {
                        vec := [1, 1, 2]
                    }
                """)
                raise RollBack

        with self.assertRaises(edgedb.errors.ConstraintViolationError):
            async with self.con.transaction():
                await self.con.execute(r"""
                    insert Con {
                        vec := [1, 20, 1]
                    }
                """)
