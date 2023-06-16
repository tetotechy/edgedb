create extension package zombodb VERSION '1.0' {
  set ext_module := "zombodb";
  set sql_extensions := ["zombodb/zdb"];
  create module zombodb;
  # HACK: The indexes ignore the actual fields specified.
  create abstract index zombodb::zombodb {
      set code := 'zombodb ((__tablename__.*))';
  };
  # XXX: Can we generalize code so as to not need two indexes here?
  create abstract index zombodb::zombodb_url(
      named only url: str
  ) {
      set code :=
        'zombodb ((__tablename__.*)) with (url = __kw_url__)';
  };
  # XXX: *Can* we make this definable reasonably?
  # (Probably not.)
  create function
  zombodb::test(
      input: anytype,
      query: std::str
  ) -> bool
  {
      SET volatility := 'Stable';
      USING SQL EXPRESSION;
  };
};
