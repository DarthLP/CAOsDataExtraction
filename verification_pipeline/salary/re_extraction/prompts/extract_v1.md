# Salary extraction prompt — v1 (patched)

This is a copy of the production `SALARY_PROMPT` from `salary_schema.py` with
**four targeted edits** to address defects found by the Phase 3b sample audit:
ragged-row column-shift, axis swap, adult inloop misclassification, and empty
placeholder rows. Edits are marked **[EDIT-N]** so they're identifiable.

---

Extract structured salary data from a JSON object derived from the Dutch CAO document.

GOAL: Produce ONE JSON object that matches the exact field names, structure, and data types defined in the Pydantic schema. Output ONLY valid JSON (UTF-8), no explanations, no hallucination, no guessing, no markdown fences, no extra text.

INPUTS
- Filename: `{filename}`
- Source text: a `wage_information` list of bullet strings (provided in the chunk).

FIELD NAME ABBREVIATIONS — see schema below.

CRITICAL RULES
- Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess.
- Missing values: Omit optional fields entirely. Only include optional fields with actual values.
- DO NOT include null values in JSON output. If an optional field has no value, omit the entire field/key from the JSON object.
- Output ONLY valid JSON format matching the provided schema structure.

TABLE SELECTION
- Include ONLY standard/regular wage tables.
- **[EDIT-1]** EXCLUDE allowances, bonuses, overtime, irregular hours, reimbursements, foremen, and entry/aanloop/starttabel/inloop scales tied to **BBL/BOL/leerling/stagiaire/in-opleiding** status.
- **[EDIT-1 cont.]** INCLUDE adult inloop/aanloop/starttabel scales labelled "21+", "22+", "23 of ouder", "volwassen", or open-ended adult (these are entry scales for NEW ADULT HIRES, not apprentices — they DO belong in structured output).
- If multiple tables exist for different worker types, time periods, education levels, job groups, steps, or age bands under this standard wage type, include all of them.
- Record the unit exactly as printed. If the same baseline is printed in multiple units for the SAME workers/period/step/education/age, choose ONE using this order: monthly > hourly > 4-week > weekly > annual.
- SKIP tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4-week vs yearly); keep ONE version (prefer monthly if present). Keep tables that differ by time period, worker type, education level, job group/function scale, steps (periodieken/trede), age bands, or contract type.

TABLE AGE GROUP SELECTION
- Create distinct SalaryRow objects for each adult-eligible age band present:
    - Open-ended adult bands (e.g., "22+", "21 and older"), OR
    - Bands that intersect ages 23-65.
- IGNORE age and job groups limited to workers under 23 (e.g., "16-20", "20") unless the group is open-ended ("20+") or spans older ages ("18-65").

TABLE JOB GROUPS, STEPS, EDUCATION, CONTRACT TYPE
- Extract ALL job groups visible in the standard wage table.
- **[EDIT-2]** AXIS LABELING: `jobgroup` holds the COLUMN-axis label (scale/function group — e.g. "A"-"J", "I"-"VII", "Schaal 5", "Functiegroep 6"). `step` holds the ROW-axis label (e.g. "0"-"10", "Periodiek 5", "Functiejaar 7", "Maximum"). If your output has jobgroup holding numeric row indices AND step holding scale letters/numbers, the axes are SWAPPED — re-read the table with the orientation fixed.
- If steps/trede (periodieken) are shown, create a separate SalaryRow per jobgroup × step × [worker type] (× [age] × [education] × [contract type]).
- If education tiers (e.g., MBO/HBO) determine different wages, create separate rows per jobgroup × step × [worker type] × education (× [age] × [contract type]).
- If contract permanency or contract hours (work arrangement) determine different wages, create separate rows per jobgroup × step × [worker type] × contract type (× [age] × [education]).
- Worker type field: OMIT if the value is generic (e.g. 'employee', 'standard worker') AND there is only one worker type in the entire CAO. KEEP when it provides meaningful distinction between different worker categories.

TABLE AMOUNTS, PERCENTAGES, DATES
- Salary amount: output as a number using a dot as the decimal separator (e.g., 2300.00). Do NOT use quotes, commas or thousands separators.
- inc_pct: include only if the table or a relating clause explicitly states a general % for that version.
- Dates: Use YYYY-MM-DD format (e.g., "2023-11-01"). Do NOT invent or infer dates.
- **[EDIT-3]** RAGGED ROWS: text data rows often have FEWER pipe-separated cells than the header has columns. Count the cells. Map values to the jobgroups whose cells are PRESENT; OMIT the SalaryRow for any (jobgroup, step) whose cell is empty or missing in that text row. NEVER pad by duplicating the rightmost value into missing trailing jobgroups. NEVER duplicate one numeric value across two adjacent jobgroups when the source supplies that value only once.
  - Example: header `[step|A|B|C|D|E|F|G|H]`, row `"8 | | | | 13.71 | 14.40 | 15.25 | 16.65"` → emit step 8 ONLY for D, E, F, G (skip A-C as left-empty; skip H — no value, do not copy G's 16.65).
  - Validation: if any (jobgroup, step) row has the same amount as the adjacent jobgroup at the same step AND that step's text row has fewer numeric cells than columns in the header, you have likely fabricated a duplicate — drop it.

TABLE TIMELINE CONSTRUCTION
- For each (jobgroup × step × [worker type] × [age] × [education] × [contract type]), build `timeline` with a SalaryPoint per table version that prints salary amounts.
- Each SalaryPoint MUST have a printed amount. If only a % increase is announced but no new amounts are printed, DO NOT add a timeline point; instead mention the % in a note.
- Use start_date exactly as the table heading or clause states, converting to YYYY-MM-DD format (e.g., "per 1 Nov 2023" → "2023-11-01"). If day is not printed, use the first day of the month, same for month.
- Align timeline points for the SAME (jobgroup × step × [worker type] × [age] × [education] × [contract type]) across time periods / table versions. Do not impute missing values.
- **[EDIT-4]** Omit any SalaryRow whose timeline would have no points. Never emit two rows for the same (jobgroup × step × worker × age × education × contract) — merge timeline points across all dates into ONE row.

WORKFLOW STEPS (INTERNAL - DO NOT OUTPUT)
1) READ & ANCHOR — Review schema below. Read source bullets to understand structure and tables.
2) LOCATE & MARK all standard wage tables per TABLE SELECTION rules.
3) IDENTIFY axis orientation per **[EDIT-2]** before extracting cells.
4) FOR EACH ROW: count text cells vs header columns per **[EDIT-3]** — never pad or duplicate.
5) DETECT AGE GROUPS within each table per TABLE AGE GROUP SELECTION.
6) DETECT STEPS / EDUCATION / CONTRACT per TABLE JOB GROUPS rules.
7) CONSTRUCT TIMELINE per TABLE TIMELINE CONSTRUCTION rules.
8) APPLY **[EDIT-4]** — drop empty-timeline rows; merge duplicates.
9) SORT each row's timeline chronologically by start_date.
10) VERIFY (SOURCE-GROUNDED) that every extracted number/date/percentage/unit is explicitly present in the input. Remove anything not grounded.
11) VALIDATE schema below.
12) OUTPUT only the final JSON.

JSON OUTPUT — SCHEMA

```jsonc
{
  "salary_information": [          // List[SalaryRow]
    {
      "jobgroup": "A",             // required str  — COLUMN axis label
      "step": "0",                 // optional str  — ROW axis label
      "worker": "construction",    // optional str  — worker type (omit if generic + single)
      "is_entry": true,            // optional bool — true for adult entry/aanloop scales
      "age_group": "23+",          // optional str  — age band as printed
      "education": "MBO",          // optional str  — education tier as printed
      "ft_hours": 38.0,            // optional float — full-time hours/week if printed
      "permanency": "permanent",   // optional str
      "hours_type": "full-time",   // optional str
      "timeline": [                // required List[SalaryPoint] — MUST be non-empty
        {
          "start_date": "2024-01-01",  // required YYYY-MM-DD
          "end_date": null,             // optional YYYY-MM-DD; omit if not stated
          "amount": 2540.0,             // required float — verbatim from text
          "unit": "monthly",            // required str  — as printed
          "table_label": "Table A - per 1 jan 2024",
          "inc_pct": 2.0,               // optional float
          "holiday_incl": true,         // optional bool
          "note": "incl. 8% vakantiegeld"
        }
      ],
      "row_note": "All amounts gross"
    }
  ]
}
```

Output ONLY this JSON. No explanations, no markdown, no preamble.
