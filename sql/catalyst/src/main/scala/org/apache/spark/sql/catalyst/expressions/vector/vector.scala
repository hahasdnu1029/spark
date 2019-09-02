/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

package object vector {

  def exprToBatch(expr: Expression): BatchExpression = {
    expr match {
      case l: Literal => BatchLiteral(l)
        case b: BoundReference => BatchBoundReference(b)
      case a: Alias => exprToBatch(a.child)

      case a @ Add(l, r) => BatchAdd(exprToBatch(l), exprToBatch(r), a)
      case s @ Subtract(l, r) => BatchSubtract(exprToBatch(l), exprToBatch(r), s)
      case m @ Multiply(l, r) => BatchMultiply(exprToBatch(l), exprToBatch(r), m)
      case ba @ BitwiseAnd(l, r) => BatchBitwiseAnd(exprToBatch(l), exprToBatch(r), ba)
      case d @ Divide(l, r) => BatchDivide(exprToBatch(l), exprToBatch(r), d)
      case p @ Pmod(l, r) => BatchPmod(exprToBatch(l), exprToBatch(r), p)

      case sw @ StartsWith(l, r) => BatchStartsWith(exprToBatch(l), exprToBatch(r), sw)
      case ew @ EndsWith(l, r) => BatchEndsWith(exprToBatch(l), exprToBatch(r), ew)
      case ct @ Contains(l, r) => BatchContains(exprToBatch(l), exprToBatch(r), ct)
      case lk @ Like(l, r) => BatchLike(exprToBatch(l), exprToBatch(r), lk)
      case i @ In(c, list) => BatchIn(exprToBatch(c), list, i)
      case is @ InSet(c, hs) => BatchInSet(exprToBatch(c), hs, is)

      case subs @ Substring(str, pos, len) =>
        BatchSubstring(exprToBatch(str), exprToBatch(pos), exprToBatch(len), subs)

      case ge @ GreaterThanOrEqual(l, r) =>
        BatchGreaterThanOrEqual(exprToBatch(l), exprToBatch(r), ge)
      case gt @ GreaterThan(l, r) =>
        BatchGreaterThan(exprToBatch(l), exprToBatch(r), gt)
      case le @ LessThanOrEqual(l, r) =>
        BatchLessThanOrEqual(exprToBatch(l), exprToBatch(r), le)
      case lt @ LessThan(l, r) =>
        BatchLessThan(exprToBatch(l), exprToBatch(r), lt)

      case e @ EqualTo(l, r) => BatchEqualTo(exprToBatch(l), exprToBatch(r), e)
      case a @ And(l, r) => BatchAnd(exprToBatch(l), exprToBatch(r), a)
      case o @ Or(l, r) => BatchOr(exprToBatch(l), exprToBatch(r), o)
      case n @ Not(l) => BatchNot(exprToBatch(l), n)

      case i @ If(p, t, f) => BatchIf(exprToBatch(p), exprToBatch(t), exprToBatch(f), i)
//      case c @ CaseWhen(exprs, _) => BatchCaseWhen(exprs.map(exprToBatch), c)

      case c @ Coalesce(children) => BatchCoalesce(children.map(exprToBatch), c)
      case c @ Cast(child, _, _) => BatchCast(exprToBatch(child), c)
      case in @ IsNull(c) => BatchIsNull(exprToBatch(c), in)
      case inn @ IsNotNull(c) => BatchIsNotNull(exprToBatch(c), inn)

      case so @ SortOrder(c, d, _, _) => BatchSortOrder(exprToBatch(c), d, so)
      case sp @ SortPrefix(c) => BatchSortPrefix(exprToBatch(c).asInstanceOf[BatchSortOrder], sp)

      case mh @ Murmur3Hash(children, seed) => BatchMurmur3Hash(children.map(exprToBatch), seed, mh)

//      case r @ Rand(seed) => BatchRand(seed, r)
//      case ri @ RandInt(seed, bound) => BatchRandInt(seed, bound, ri)

//      case v @ V2R(children) => VectorsToRow(children.map(exprToBatch), v)

      case _ =>
        throw new UnsupportedOperationException(s"unable to convert $expr to its batch version")
    }
  }
}
