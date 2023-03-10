﻿using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Parquet.Schema;

namespace Parquet.Serialization.Dremel {
    class FieldStriper<TClass> : FieldWorker<TClass> {

        public FieldStriper(DataField field, Func<DataField, IEnumerable<TClass>, ShreddedColumn> striper,
            Expression expression, Expression iterationExpression)
            : base(field, expression, iterationExpression) {
            Stripe = striper;
        }

        public Func<DataField, IEnumerable<TClass>, ShreddedColumn> Stripe { get; }
    }
}
