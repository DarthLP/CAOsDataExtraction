"""qa.shared — topic-agnostic reusable code for the CAO QA pipeline.

Imports allow `from qa.shared import aggregator_lib` etc. from anywhere.

Most modules are signature stubs as of Phase 1; implementations port from
qa_leave/scripts/ in Phase 2. See qa/EXPERT_IMPLEMENTATION_PLAN.md.

Already implemented in Phase 1:
  - logging_util.log_event             (small, low-risk; used by everything)
  - worksheet_builder.BudgetExceeded   (real exception class)
  - source_text_loader.TOPIC_TO_FILENAME  (real constant)
  - topic_keywords.TOPIC_KEYWORDS      (real constant, seeded from schema)
"""
