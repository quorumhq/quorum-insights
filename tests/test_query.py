"""Tests for the query builder layer.

These tests validate SQL generation and parameterization.
No live ClickHouse required — we test the SQL strings.
"""

from datetime import date

import pytest

from query.retention import RetentionQuery, RetentionPeriod
from query.funnel import FunnelQuery, FunnelStep
from query.cohort import CohortQuery, CohortBy
from query.metrics import MetricsQuery


# ─── Retention Tests ───


class TestRetentionQuery:
    """Test retention query builder."""

    def _default(self, **kwargs) -> RetentionQuery:
        defaults = dict(
            tenant_id="t1",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 3, 31),
            period=RetentionPeriod.WEEK,
            num_periods=4,
        )
        defaults.update(kwargs)
        return RetentionQuery(**defaults)

    def test_build_returns_sql_and_params(self):
        q = self._default()
        sql, params = q.build()
        assert isinstance(sql, str)
        assert isinstance(params, dict)

    def test_params_match_placeholders(self):
        q = self._default()
        sql, params = q.build()
        assert params["tenant_id"] == "t1"
        assert params["start_date"] == "2026-01-01"
        assert params["end_date"] == "2026-03-31"

    def test_uses_retention_function(self):
        q = self._default()
        sql, _ = q.build()
        assert "retention(" in sql

    def test_weekly_uses_toStartOfWeek(self):
        q = self._default(period=RetentionPeriod.WEEK)
        sql, _ = q.build()
        assert "toStartOfWeek" in sql

    def test_daily_uses_toDate(self):
        q = self._default(period=RetentionPeriod.DAY)
        sql, _ = q.build()
        assert "toDate" in sql

    def test_monthly_uses_toStartOfMonth(self):
        q = self._default(period=RetentionPeriod.MONTH)
        sql, _ = q.build()
        assert "toStartOfMonth" in sql

    def test_num_periods_generates_conditions(self):
        q = self._default(num_periods=6)
        sql, _ = q.build()
        # Should have 6 INTERVAL conditions
        assert sql.count("INTERVAL") >= 6

    def test_event_filter_included(self):
        q = self._default(event_filter="event_name = 'login'")
        sql, _ = q.build()
        assert "event_name = 'login'" in sql

    def test_ai_only_filter(self):
        q = self._default(ai_only=True)
        sql, _ = q.build()
        assert "ai_model != ''" in sql

    def test_build_simple_no_retention_function(self):
        q = self._default()
        sql, params = q.build_simple()
        assert "retention(" not in sql
        assert "countDistinctIf" in sql
        assert params["tenant_id"] == "t1"

    def test_build_simple_generates_period_columns(self):
        q = self._default(num_periods=4)
        sql, _ = q.build_simple()
        assert "period_0" in sql
        assert "period_3" in sql

    def test_user_id_filter(self):
        """Both build variants filter out empty user_id."""
        q = self._default()
        sql1, _ = q.build()
        sql2, _ = q.build_simple()
        assert "user_id != ''" in sql1
        assert "user_id != ''" in sql2


# ─── Funnel Tests ───


class TestFunnelQuery:
    """Test funnel query builder."""

    def _steps(self) -> list[FunnelStep]:
        return [
            FunnelStep("signup", "event_name = 'signup'"),
            FunnelStep("activate", "event_name = 'first_action'"),
            FunnelStep("subscribe", "event_name = 'subscription_started'"),
        ]

    def _default(self, **kwargs) -> FunnelQuery:
        defaults = dict(
            tenant_id="t1",
            steps=self._steps(),
            window_seconds=7 * 86400,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 3, 31),
        )
        defaults.update(kwargs)
        return FunnelQuery(**defaults)

    def test_build_returns_sql_and_params(self):
        q = self._default()
        sql, params = q.build()
        assert isinstance(sql, str)
        assert params["tenant_id"] == "t1"

    def test_uses_windowFunnel(self):
        q = self._default()
        sql, _ = q.build()
        assert "windowFunnel(" in sql

    def test_window_seconds_in_query(self):
        q = self._default(window_seconds=86400)
        sql, _ = q.build()
        assert "windowFunnel(86400)" in sql

    def test_step_conditions_in_query(self):
        q = self._default()
        sql, _ = q.build()
        assert "event_name = 'signup'" in sql
        assert "event_name = 'first_action'" in sql
        assert "event_name = 'subscription_started'" in sql

    def test_step_labels_in_column_names(self):
        q = self._default()
        sql, _ = q.build()
        assert "step_1_signup" in sql
        assert "step_2_activate" in sql
        assert "step_3_subscribe" in sql

    def test_by_date_includes_entry_date(self):
        q = self._default()
        sql, _ = q.build_by_date()
        assert "entry_date" in sql
        assert "GROUP BY entry_date" in sql
        assert "ORDER BY entry_date" in sql

    def test_min_steps_validation(self):
        with pytest.raises(ValueError, match="at least 2"):
            FunnelQuery(
                tenant_id="t1",
                steps=[FunnelStep("only", "event_name = 'x'")],
            )

    def test_max_steps_validation(self):
        steps = [FunnelStep(f"s{i}", f"event_name = 'e{i}'") for i in range(21)]
        with pytest.raises(ValueError, match="20 steps"):
            FunnelQuery(tenant_id="t1", steps=steps)

    def test_empty_label_rejected(self):
        with pytest.raises(ValueError, match="label"):
            FunnelStep("", "event_name = 'x'")

    def test_empty_condition_rejected(self):
        with pytest.raises(ValueError, match="condition"):
            FunnelStep("ok", "")

    def test_window_seconds_positive(self):
        with pytest.raises(ValueError, match="positive"):
            FunnelQuery(tenant_id="t1", steps=self._steps(), window_seconds=0)

    def test_segment_filter(self):
        q = self._default(segment_filter="country = 'US'")
        sql, _ = q.build()
        assert "country = 'US'" in sql

    def test_step_labels(self):
        q = self._default()
        assert q.step_labels() == ["signup", "activate", "subscribe"]


# ─── Cohort Tests ───


class TestCohortQuery:
    """Test cohort query builder."""

    def _default(self, **kwargs) -> CohortQuery:
        defaults = dict(
            tenant_id="t1",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 3, 31),
            cohort_by=CohortBy.FIRST_SEEN_WEEK,
        )
        defaults.update(kwargs)
        return CohortQuery(**defaults)

    def test_build_returns_sql_and_params(self):
        q = self._default()
        sql, params = q.build()
        assert isinstance(sql, str)
        assert params["tenant_id"] == "t1"

    def test_first_seen_week(self):
        q = self._default(cohort_by=CohortBy.FIRST_SEEN_WEEK)
        sql, _ = q.build()
        assert "toStartOfWeek" in sql
        assert "cohort_date" in sql

    def test_first_seen_month(self):
        q = self._default(cohort_by=CohortBy.FIRST_SEEN_MONTH)
        sql, _ = q.build()
        assert "toStartOfMonth" in sql

    def test_first_seen_day(self):
        q = self._default(cohort_by=CohortBy.FIRST_SEEN_DAY)
        sql, _ = q.build()
        assert "toDate" in sql

    def test_first_event_cohort(self):
        q = self._default(cohort_by=CohortBy.FIRST_EVENT)
        sql, _ = q.build()
        assert "argMin(e.event_name, e.timestamp)" in sql
        assert "first_event" in sql

    def test_property_cohort(self):
        q = self._default(cohort_by=CohortBy.PROPERTY, property_key="plan")
        sql, _ = q.build()
        assert "user_properties_set['plan']" in sql

    def test_property_cohort_requires_key(self):
        with pytest.raises(ValueError, match="property_key"):
            CohortQuery(
                tenant_id="t1",
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                cohort_by=CohortBy.PROPERTY,
            )

    def test_includes_ai_metrics(self):
        q = self._default()
        sql, _ = q.build()
        assert "ai_event_count" in sql
        assert "avg_ai_events_per_user" in sql

    def test_includes_lifespan(self):
        q = self._default()
        sql, _ = q.build()
        assert "avg_lifespan_days" in sql

    def test_limit_cohorts(self):
        q = self._default(limit_cohorts=10)
        sql, _ = q.build()
        assert "LIMIT 10" in sql

    def test_event_filter(self):
        q = self._default(event_filter="event_type = 'track'")
        sql, _ = q.build()
        assert "event_type = 'track'" in sql

    def test_comparison_query(self):
        q = self._default()
        sql, params = q.build_comparison("purchase_completed")
        assert "metric_event" in params or "{metric_event:String}" in sql
        assert params["metric_event"] == "purchase_completed"
        assert "metric_rate" in sql

    def test_comparison_includes_cohort_size(self):
        q = self._default()
        sql, _ = q.build_comparison("upgrade")
        assert "cohort_size" in sql
        assert "users_with_metric" in sql


# ─── Metrics Tests ───


class TestMetricsQuery:
    """Test metrics query builder (reads from MVs)."""

    def _default(self) -> MetricsQuery:
        return MetricsQuery(
            tenant_id="t1",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 3, 31),
        )

    def test_daily_active_users(self):
        sql, params = self._default().daily_active_users()
        assert "uniqMerge(unique_users)" in sql
        assert "daily_metrics_mv" in sql
        assert params["tenant_id"] == "t1"

    def test_daily_metrics_by_type(self):
        sql, params = self._default().daily_metrics_by_type()
        assert "countMerge(event_count)" in sql
        assert "uniqMerge(unique_users)" in sql
        assert "avgMerge(avg_ai_quality)" in sql
        assert "GROUP BY event_date, event_type" in sql

    def test_feature_usage_ranking(self):
        sql, _ = self._default().feature_usage_ranking(limit=20)
        assert "feature_usage_mv" in sql
        assert "uniqMerge(unique_users)" in sql
        assert "sumMerge(total_ai_cost)" in sql
        assert "LIMIT 20" in sql

    def test_feature_trend(self):
        sql, params = self._default().feature_trend("button_click")
        assert "feature_usage_mv" in sql
        assert params["event_name"] == "button_click"
        assert "ORDER BY event_date" in sql

    def test_user_profile(self):
        sql, params = self._default().user_profile("user-123")
        assert "user_profiles_mv" in sql
        assert params["user_id"] == "user-123"
        assert "minMerge(first_seen)" in sql
        assert "maxMerge(last_seen)" in sql
        assert "countMerge(event_count)" in sql
        assert "groupUniqArrayMerge(ai_features_used)" in sql
        assert "groupUniqArrayMerge(source_systems)" in sql

    def test_user_cohort_info(self):
        sql, params = self._default().user_cohort_info("user-456")
        assert "user_cohorts_mv" in sql
        assert params["user_id"] == "user-456"
        assert "minMerge(cohort_date)" in sql
        assert "countMerge(lifetime_events)" in sql

    def test_overview(self):
        sql, _ = self._default().overview()
        assert "daily_metrics_mv" in sql
        assert "total_events" in sql
        assert "total_users" in sql
        assert "total_ai_events" in sql

    def test_recent_events(self):
        sql, params = self._default().recent_events(limit=50)
        assert "events_recent" in sql
        assert "LIMIT 50" in sql
        assert params["tenant_id"] == "t1"
        # recent_events only needs tenant_id, not date range
        assert "start_date" not in params

    def test_all_mv_queries_use_merge_combinators(self):
        """Every MV query must use -Merge combinators, not plain aggregates."""
        mq = self._default()
        mv_queries = [
            mq.daily_active_users,
            mq.daily_metrics_by_type,
            mq.feature_usage_ranking,
            mq.overview,
        ]
        for qfn in mv_queries:
            sql, _ = qfn()
            # Should NOT contain plain count(), uniq(), avg() on MV columns
            # Should contain countMerge, uniqMerge, avgMerge
            assert "Merge(" in sql, f"{qfn.__name__} missing -Merge combinator"


# ─── SQL Syntax Sanity Tests ───


class TestSQLSanity:
    """Basic SQL syntax sanity checks across all builders."""

    def test_no_semicolons_in_queries(self):
        """Query strings should not end with semicolons (driver adds them)."""
        queries = [
            RetentionQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).build(),
            RetentionQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).build_simple(),
            FunnelQuery(
                "t1",
                [FunnelStep("a", "event_name='a'"), FunnelStep("b", "event_name='b'")],
            ).build(),
            CohortQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).build(),
            MetricsQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).daily_active_users(),
        ]
        for sql, _ in queries:
            assert not sql.rstrip().endswith(";"), f"SQL ends with semicolon: {sql[-50:]}"

    def test_all_queries_have_tenant_filter(self):
        """Every query must filter by tenant_id for multi-tenant safety."""
        queries = [
            RetentionQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).build(),
            RetentionQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).build_simple(),
            FunnelQuery(
                "t1",
                [FunnelStep("a", "event_name='a'"), FunnelStep("b", "event_name='b'")],
            ).build(),
            CohortQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).build(),
            MetricsQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).daily_active_users(),
            MetricsQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).overview(),
            MetricsQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).recent_events(),
        ]
        for sql, params in queries:
            assert "tenant_id" in sql, f"Missing tenant_id filter in: {sql[:100]}"
            assert "tenant_id" in params

    def test_all_params_are_strings(self):
        """All param values should be strings (ClickHouse parameterized format)."""
        queries = [
            RetentionQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).build(),
            FunnelQuery(
                "t1",
                [FunnelStep("a", "event_name='a'"), FunnelStep("b", "event_name='b'")],
            ).build(),
            CohortQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).build(),
            MetricsQuery("t1", date(2026, 1, 1), date(2026, 3, 31)).daily_active_users(),
        ]
        for _, params in queries:
            for k, v in params.items():
                assert isinstance(v, str), f"Param {k} is {type(v)}, expected str"
