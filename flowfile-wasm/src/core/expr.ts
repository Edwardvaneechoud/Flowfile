import pl from 'nodejs-polars';

/**
 * FlowExpr - Expression wrapper that mirrors Python FlowFrame Expr style
 * Wraps nodejs-polars expressions with a fluent API
 */
export class FlowExpr {
  private _expr: pl.Expr;

  constructor(expr: pl.Expr) {
    this._expr = expr;
  }

  /**
   * Get the underlying Polars expression
   */
  get expr(): pl.Expr {
    return this._expr;
  }

  // Comparison operators
  eq(other: FlowExpr | any): FlowExpr {
    const otherExpr = other instanceof FlowExpr ? other._expr : pl.lit(other);
    return new FlowExpr(this._expr.eq(otherExpr));
  }

  neq(other: FlowExpr | any): FlowExpr {
    const otherExpr = other instanceof FlowExpr ? other._expr : pl.lit(other);
    return new FlowExpr(this._expr.neq(otherExpr));
  }

  gt(other: FlowExpr | any): FlowExpr {
    const otherExpr = other instanceof FlowExpr ? other._expr : pl.lit(other);
    return new FlowExpr(this._expr.gt(otherExpr));
  }

  gte(other: FlowExpr | any): FlowExpr {
    const otherExpr = other instanceof FlowExpr ? other._expr : pl.lit(other);
    return new FlowExpr(this._expr.gtEq(otherExpr));
  }

  lt(other: FlowExpr | any): FlowExpr {
    const otherExpr = other instanceof FlowExpr ? other._expr : pl.lit(other);
    return new FlowExpr(this._expr.lt(otherExpr));
  }

  lte(other: FlowExpr | any): FlowExpr {
    const otherExpr = other instanceof FlowExpr ? other._expr : pl.lit(other);
    return new FlowExpr(this._expr.ltEq(otherExpr));
  }

  // Logical operators
  and(other: FlowExpr): FlowExpr {
    return new FlowExpr(this._expr.and(other._expr));
  }

  or(other: FlowExpr): FlowExpr {
    return new FlowExpr(this._expr.or(other._expr));
  }

  not(): FlowExpr {
    return new FlowExpr(this._expr.not());
  }

  // Arithmetic operators
  add(other: FlowExpr | any): FlowExpr {
    const otherExpr = other instanceof FlowExpr ? other._expr : pl.lit(other);
    return new FlowExpr(this._expr.add(otherExpr));
  }

  sub(other: FlowExpr | any): FlowExpr {
    const otherExpr = other instanceof FlowExpr ? other._expr : pl.lit(other);
    return new FlowExpr(this._expr.sub(otherExpr));
  }

  mul(other: FlowExpr | any): FlowExpr {
    const otherExpr = other instanceof FlowExpr ? other._expr : pl.lit(other);
    return new FlowExpr(this._expr.mul(otherExpr));
  }

  div(other: FlowExpr | any): FlowExpr {
    const otherExpr = other instanceof FlowExpr ? other._expr : pl.lit(other);
    return new FlowExpr(this._expr.div(otherExpr));
  }

  // Aggregations
  sum(): FlowExpr {
    return new FlowExpr(this._expr.sum());
  }

  mean(): FlowExpr {
    return new FlowExpr(this._expr.mean());
  }

  min(): FlowExpr {
    return new FlowExpr(this._expr.min());
  }

  max(): FlowExpr {
    return new FlowExpr(this._expr.max());
  }

  count(): FlowExpr {
    return new FlowExpr(this._expr.count());
  }

  first(): FlowExpr {
    return new FlowExpr(this._expr.first());
  }

  last(): FlowExpr {
    return new FlowExpr(this._expr.last());
  }

  // String operations
  str = {
    contains: (pattern: string): FlowExpr => {
      return new FlowExpr(this._expr.str.contains(pattern));
    },
    startsWith: (prefix: string): FlowExpr => {
      // Use regex for startsWith if not available
      return new FlowExpr(this._expr.str.contains(`^${prefix}`));
    },
    endsWith: (suffix: string): FlowExpr => {
      // Use regex for endsWith if not available
      return new FlowExpr(this._expr.str.contains(`${suffix}$`));
    },
    toUppercase: (): FlowExpr => {
      return new FlowExpr(this._expr.str.toUpperCase());
    },
    toLowercase: (): FlowExpr => {
      return new FlowExpr(this._expr.str.toLowerCase());
    },
    replace: (pattern: string, value: string): FlowExpr => {
      return new FlowExpr(this._expr.str.replace(pattern, value));
    },
    slice: (start: number, length?: number): FlowExpr => {
      return new FlowExpr(this._expr.str.slice(start, length));
    },
    lengths: (): FlowExpr => {
      return new FlowExpr(this._expr.str.lengths());
    }
  };

  // Type casting
  cast(dtype: any): FlowExpr {
    return new FlowExpr(this._expr.cast(dtype));
  }

  // Alias
  alias(name: string): FlowExpr {
    return new FlowExpr(this._expr.alias(name));
  }

  // Null handling
  isNull(): FlowExpr {
    return new FlowExpr(this._expr.isNull());
  }

  isNotNull(): FlowExpr {
    return new FlowExpr(this._expr.isNotNull());
  }

  fillNull(value: any): FlowExpr {
    return new FlowExpr(this._expr.fillNull(value));
  }
}

/**
 * Create a column expression (like pl.col())
 */
export function col(name: string): FlowExpr {
  return new FlowExpr(pl.col(name));
}

/**
 * Create a literal expression (like pl.lit())
 */
export function lit(value: any): FlowExpr {
  return new FlowExpr(pl.lit(value));
}
