#!/usr/bin/env python3
"""Generate RM Pipeline Dashboard with fresh data from PostgreSQL.
v3: includes Fortnox financial data with label breakdown for overdue invoices."""
import json, sys, re
from datetime import datetime

# --- rm_data module imports ---
sys.path.insert(0, '/opt/rm-infra')
from rm_data import query_one, query_all, safe_json_query

# Get deals
deals_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
  SELECT name, stage, deal_type,
    COALESCE(estimated_value,0)::bigint as estimated_value,
    COALESCE(calculated_value,0)::bigint as calculated_value,
    COALESCE(hit_rate,25)::int as hit_rate,
    twenty_id, COALESCE(customer_name,'') as customer,
    COALESCE(lead_source,'') as lead_source,
    COALESCE(contract_type,'') as contract_type,
    COALESCE(region,'') as region,
    COALESCE(margin,0) as margin,
    COALESCE(probability,0) as probability,
    COALESCE(next_project_no,'') as next_project_no
  FROM pipeline_deal WHERE company_code='RM'
  ORDER BY estimated_value DESC
) t
""")
deals = deals_raw

# Get hitrate matrix
hr_raw = safe_json_query("""
SELECT json_object_agg(deal_type, stages) FROM (
  SELECT deal_type, json_object_agg(stage, hitrate::int) as stages
  FROM hitrate_matrix GROUP BY deal_type
) t
""")
hitrate = hr_raw if hr_raw else {}

# Get Fortnox financial summary
fortnox = {}
try:
    inv_count = query_one("SELECT COUNT(*) FROM fortnox_invoice WHERE company_code='RM'")
    if int(inv_count or 0) > 0:
        fortnox['revenue'] = float(query_one("SELECT COALESCE(SUM(total),0) FROM fortnox_invoice WHERE company_code='RM' AND NOT is_credit AND status != 'cancelled' AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'") or 0)
        fortnox['credit_notes'] = float(query_one("SELECT COALESCE(SUM(total),0) FROM fortnox_invoice WHERE company_code='RM' AND is_credit AND status != 'cancelled' AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'") or 0)
        fortnox['net_revenue'] = fortnox['revenue'] - abs(fortnox['credit_notes'])
        fortnox['receivables'] = float(query_one("SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='RM' AND balance > 0 AND status != 'cancelled'") or 0)
        fortnox['overdue_receivables'] = float(query_one("SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='RM' AND balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled'") or 0)
        # Label breakdown for overdue
        fortnox['overdue_real'] = float(query_one("SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='RM' AND balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled' AND (label IS NULL OR label = '' OR label = 'Bevakas')") or 0)
        fortnox['overdue_labeled'] = float(query_one("SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='RM' AND balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled' AND label IN ('Parkerad','Tvist')") or 0)

        # CFO risk classification of receivables
        fortnox['recv_safe'] = float(query_one("SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='RM' AND balance > 0 AND status != 'cancelled' AND due_date >= CURRENT_DATE AND (label IS NULL OR label = '')") or 0)
        fortnox['recv_risk'] = float(query_one("SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='RM' AND balance > 0 AND status != 'cancelled' AND label = 'Bevakas'") or 0)
        fortnox['recv_critical'] = float(query_one("SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='RM' AND balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled' AND (label IS NULL OR label = '' OR label = 'Bevakas')") or 0)
        fortnox['recv_parked'] = float(query_one("SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='RM' AND balance > 0 AND status != 'cancelled' AND label = 'Parkerad'") or 0)
        # Risk-weighted receivables: safe=100%, risk=50%, critical=25%, parked=0%
        fortnox['recv_weighted'] = fortnox['recv_safe'] + fortnox['recv_risk'] * 0.5 + fortnox['recv_critical'] * 0.25

        fortnox['supplier_costs'] = float(query_one("SELECT COALESCE(SUM(total),0) FROM fortnox_supplier_invoice WHERE company_code='RM' AND status != 'cancelled' AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'") or 0)
        fortnox['payables'] = float(query_one("SELECT COALESCE(SUM(balance),0) FROM fortnox_supplier_invoice WHERE company_code='RM' AND balance > 0 AND status != 'cancelled'") or 0)
        fortnox['overdue_payables'] = float(query_one("SELECT COALESCE(SUM(balance),0) FROM fortnox_supplier_invoice WHERE company_code='RM' AND balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled'") or 0)
        fortnox['tb1'] = fortnox['net_revenue'] - fortnox['supplier_costs']
        fortnox['tb1_margin'] = (fortnox['tb1'] / fortnox['net_revenue'] * 100) if fortnox['net_revenue'] > 0 else 0
        fortnox['invoice_count'] = int(query_one("SELECT COUNT(*) FROM fortnox_invoice WHERE company_code='RM' AND NOT is_credit AND status != 'cancelled' AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'") or 0)
        fortnox['supplier_invoice_count'] = int(query_one("SELECT COUNT(*) FROM fortnox_supplier_invoice WHERE company_code='RM' AND status != 'cancelled'") or 0)
        # Top 5 customers by revenue
        top_cust_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT customer_name as name, SUM(total)::numeric(14,0) as total, COUNT(*) as count
                FROM fortnox_invoice WHERE company_code='RM' AND NOT is_credit AND status != 'cancelled' AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'
                GROUP BY customer_name ORDER BY SUM(total) DESC LIMIT 5
            ) t
        """)
        fortnox['top_customers'] = top_cust_raw if top_cust_raw else []
        # Top 5 suppliers by cost
        top_sup_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT supplier_name as name, SUM(total)::numeric(14,0) as total, COUNT(*) as count
                FROM fortnox_supplier_invoice WHERE company_code='RM' AND status != 'cancelled' AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'
                GROUP BY supplier_name ORDER BY SUM(total) DESC LIMIT 5
            ) t
        """)
        fortnox['top_suppliers'] = top_sup_raw if top_sup_raw else []
        # Overdue invoice details (for table)
        overdue_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT fortnox_id, customer_name, due_date::text, balance::numeric(14,0), COALESCE(label,'') as label
                FROM fortnox_invoice WHERE company_code='RM' AND status='overdue' AND balance > 0
                ORDER BY balance DESC
            ) t
        """)
        fortnox['overdue_invoices'] = overdue_raw if overdue_raw else []

        # ALL customer invoices with outstanding balance
        all_inv_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT fortnox_id, customer_name, invoice_date::text, due_date::text, total::numeric(14,0), balance::numeric(14,0),
                       COALESCE(label,'') as label, status,
                       CASE WHEN due_date < CURRENT_DATE THEN true ELSE false END as is_overdue
                FROM fortnox_invoice
                WHERE company_code='RM' AND balance > 0 AND status != 'cancelled'
                ORDER BY label NULLS LAST, due_date
            ) t
        """)
        fortnox['all_invoices'] = all_inv_raw if all_inv_raw else []
        fortnox['inv_parkerad_count'] = len([i for i in fortnox['all_invoices'] if i.get('label') == 'Parkerad'])
        fortnox['inv_parkerad_total'] = sum(float(i['balance']) for i in fortnox['all_invoices'] if i.get('label') == 'Parkerad')
        fortnox['inv_bevakas_count'] = len([i for i in fortnox['all_invoices'] if i.get('label') == 'Bevakas'])
        fortnox['inv_bevakas_total'] = sum(float(i['balance']) for i in fortnox['all_invoices'] if i.get('label') == 'Bevakas')
        fortnox['inv_unpaid_count'] = len([i for i in fortnox['all_invoices'] if not i.get('is_overdue') and not i.get('label')])
        fortnox['inv_unpaid_total'] = sum(float(i['balance']) for i in fortnox['all_invoices'] if not i.get('is_overdue') and not i.get('label'))
        fortnox['inv_overdue_unlabeled_count'] = len([i for i in fortnox['all_invoices'] if i.get('is_overdue') and not i.get('label')])
        fortnox['inv_overdue_unlabeled_total'] = sum(float(i['balance']) for i in fortnox['all_invoices'] if i.get('is_overdue') and not i.get('label'))

        # Overdue supplier invoice details
        overdue_si_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT fortnox_id, supplier_name, due_date::text, balance::numeric(14,0), COALESCE(label,'') as label
                FROM fortnox_supplier_invoice WHERE company_code='RM' AND balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled'
                ORDER BY balance DESC LIMIT 10
            ) t
        """)
        fortnox['overdue_supplier_invoices'] = overdue_si_raw if overdue_si_raw else []

        # ALL supplier invoices with outstanding balance (for label breakdown: Parkerad, Bevakas, Forfallna)
        all_si_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT fortnox_id, supplier_name, due_date::text, balance::numeric(14,0),
                       COALESCE(label,'') as label,
                       CASE WHEN due_date < CURRENT_DATE THEN true ELSE false END as is_overdue
                FROM fortnox_supplier_invoice
                WHERE company_code='RM' AND balance > 0 AND status != 'cancelled'
                ORDER BY label NULLS LAST, due_date
            ) t
        """)
        fortnox['all_supplier_invoices'] = all_si_raw if all_si_raw else []

        # Label breakdown summaries for supplier invoices
        fortnox['si_parkerad_count'] = len([i for i in fortnox['all_supplier_invoices'] if i.get('label') == 'Parkerad'])
        fortnox['si_parkerad_total'] = sum(float(i['balance']) for i in fortnox['all_supplier_invoices'] if i.get('label') == 'Parkerad')
        fortnox['si_bevakas_count'] = len([i for i in fortnox['all_supplier_invoices'] if i.get('label') == 'Bevakas'])
        fortnox['si_bevakas_total'] = sum(float(i['balance']) for i in fortnox['all_supplier_invoices'] if i.get('label') == 'Bevakas')
        fortnox['si_overdue_unlabeled_count'] = len([i for i in fortnox['all_supplier_invoices'] if i.get('is_overdue') and not i.get('label')])
        fortnox['si_overdue_unlabeled_total'] = sum(float(i['balance']) for i in fortnox['all_supplier_invoices'] if i.get('is_overdue') and not i.get('label'))

        # ── Likviditetsprognos (kassaflöde per vecka) ────────────
        # Risk-weighted receivables per week:
        #   Safe (ej förfallen, ej etiketterad) = 100%
        #   Bevakas = 50%
        #   Förfallen utan etikett = 25%
        #   Parkerad = 0% (exkluderad)
        forecast_recv_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT
                    CASE WHEN due_date < CURRENT_DATE THEN 0
                         ELSE GREATEST(EXTRACT(WEEK FROM due_date) - EXTRACT(WEEK FROM CURRENT_DATE)
                              + 52 * (EXTRACT(YEAR FROM due_date) - EXTRACT(YEAR FROM CURRENT_DATE)), 0)
                    END::int as week_offset,
                    SUM(balance)::numeric(14,0) as amount,
                    SUM(CASE
                        WHEN label = 'Parkerad' THEN 0
                        WHEN label = 'Tvist' THEN 0
                        WHEN label = 'Bevakas' THEN balance * 0.5
                        WHEN due_date < CURRENT_DATE AND (label IS NULL OR label = '') THEN balance * 0.25
                        ELSE balance
                    END)::numeric(14,0) as weighted_amount
                FROM fortnox_invoice
                WHERE company_code='RM' AND balance > 0 AND status != 'cancelled'
                GROUP BY 1 ORDER BY 1
            ) t
        """)
        forecast_recv = forecast_recv_raw

        # Utbetalningar: leverantörsskulder per vecka
        # Parkerade (väntar på kundbetalning) exkluderas helt
        forecast_pay_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT
                    CASE WHEN due_date < CURRENT_DATE THEN 0
                         ELSE GREATEST(EXTRACT(WEEK FROM due_date) - EXTRACT(WEEK FROM CURRENT_DATE)
                              + 52 * (EXTRACT(YEAR FROM due_date) - EXTRACT(YEAR FROM CURRENT_DATE)), 0)
                    END::int as week_offset,
                    SUM(balance)::numeric(14,0) as amount,
                    SUM(CASE
                        WHEN label = 'Parkerad' THEN 0
                        WHEN label = 'Tvist' THEN 0
                        WHEN label = 'Bevakas' THEN balance * 0.5
                        ELSE balance
                    END)::numeric(14,0) as adjusted_amount
                FROM fortnox_supplier_invoice
                WHERE company_code='RM' AND balance > 0 AND status != 'cancelled'
                GROUP BY 1 ORDER BY 1
            ) t
        """)
        forecast_pay = forecast_pay_raw

        # Build weekly arrays for up to 52 weeks
        import datetime as _dt
        current_week = int(_dt.date.today().isocalendar()[1])
        current_year = _dt.date.today().year
        forecast_weeks = []
        recv_by_week = {int(r['week_offset']): float(r['amount']) for r in forecast_recv}
        recv_weighted_by_week = {int(r['week_offset']): float(r['weighted_amount']) for r in forecast_recv}
        pay_by_week = {int(r['week_offset']): float(r['amount']) for r in forecast_pay}
        pay_adj_by_week = {int(r['week_offset']): float(r['adjusted_amount']) for r in forecast_pay}

        for w in range(52):
            abs_week = ((current_week - 1 + w) % 52) + 1
            forecast_weeks.append({
                'label': f'V{abs_week}',
                'inbetalningar': recv_by_week.get(w, 0),
                'inbetalningar_viktat': recv_weighted_by_week.get(w, 0),
                'utbetalningar': pay_by_week.get(w, 0),
                'utbetalningar_justerat': pay_adj_by_week.get(w, 0)
            })


        # Project profitability data — individual projects
        projects_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT project_number, project_name, COALESCE(project_group,'') as project_group,
                       COALESCE(project_leader,'') as project_leader,
                       revenue::numeric(14,0), net_revenue::numeric(14,0), 
                       supplier_costs::numeric(14,0), tb1::numeric(14,0), tb1_margin, 
                       invoice_count, supplier_invoice_count
                FROM project_profitability 
                WHERE company_code='RM' AND status='ONGOING' AND (net_revenue > 0 OR supplier_costs > 0)
                ORDER BY supplier_costs DESC
            ) t
        """)
        fortnox['projects'] = projects_raw

        # Project profitability data — grouped by project_group
        grouped_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT COALESCE(NULLIF(project_group,''), project_name) as group_name,
                       SUM(net_revenue)::numeric(14,0) as net_revenue,
                       SUM(supplier_costs)::numeric(14,0) as supplier_costs,
                       SUM(tb1)::numeric(14,0) as tb1,
                       CASE WHEN SUM(net_revenue) > 0 
                            THEN ((SUM(tb1) / SUM(net_revenue)) * 100)::numeric(6,1)
                            ELSE 0 END as tb1_margin,
                       SUM(invoice_count) as invoice_count,
                       SUM(supplier_invoice_count) as supplier_invoice_count,
                       string_agg(DISTINCT project_leader, ', ') FILTER (WHERE project_leader != '') as leaders
                FROM project_unified
                WHERE company_code='RM' AND fortnox_status='ONGOING' AND (net_revenue > 0 OR supplier_costs > 0)
                GROUP BY COALESCE(NULLIF(project_group,''), project_name)
                ORDER BY supplier_costs DESC
            ) t
        """)
        fortnox['project_groups'] = grouped_raw

        fortnox['forecast_weeks'] = forecast_weeks

        # Intäktsprognos — historiskt snitt av fakturerad omsättning per månad
        # Beräknas från faktiska fakturor, 6 senaste kompletta månaderna
        revenue_history_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT 
                    to_char(invoice_date, 'YYYY-MM') as month,
                    SUM(CASE WHEN NOT is_credit THEN total ELSE -total END)::numeric(14,0) as netto
                FROM fortnox_invoice 
                WHERE company_code='RM' 
                    AND status != 'cancelled'
                    AND invoice_date >= date_trunc('month', CURRENT_DATE) - interval '6 months'
                    AND invoice_date < date_trunc('month', CURRENT_DATE)
                GROUP BY 1
                ORDER BY 1
            ) t
        """)
        revenue_history = revenue_history_raw
        fortnox['revenue_history'] = revenue_history
        
        if revenue_history:
            avg_monthly = sum(float(r['netto']) for r in revenue_history) / len(revenue_history)
            fortnox['revenue_avg_monthly'] = round(avg_monthly)
            fortnox['revenue_avg_weekly'] = round(avg_monthly / 4.33)
            fortnox['revenue_months_used'] = len(revenue_history)
        else:
            fortnox['revenue_avg_monthly'] = 0
            fortnox['revenue_avg_weekly'] = 0
            fortnox['revenue_months_used'] = 0


        # Löner & AGI — datumbaserade kassaflödeshändelser
        # Verifierade belopp från Fortnox lönekörning och arbetsgivardeklaration:
        #   Nettolön: ~180 150 kr, utbetalas den 25:e varje månad
        #   SKV (AGI + personalskatt): ~126 218 kr, betalas den 12:e månaden efter
        # Dessa belopp uppdateras manuellt vid löneförändringar.
        salary_nettolon = 180150   # Nettolön per månad (feb 2026: M.03 132 768 + K.02 47 382)
        salary_skv = 126218        # AGI+skatt per månad (medel S.01: 129 554, S.02: 122 882)

        salary_events = []
        today = _dt.date.today()
        for month_offset in range(0, 13):
            # Nettolön: 25:e i varje kommande månad
            m = today.month + month_offset
            y = today.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            try:
                pay_date = _dt.date(y, m, 25)
            except ValueError:
                pay_date = _dt.date(y, m, 28)  # feb edge case
            if pay_date >= today:
                salary_events.append({
                    'date': pay_date.isoformat(),
                    'type': 'nettolon',
                    'amount': salary_nettolon,
                    'label': f'Nettolön {pay_date.strftime("%b")}'
                })

            # SKV: 12:e i varje kommande månad (avser föregående månads AGI+skatt)
            try:
                skv_date = _dt.date(y, m, 12)
            except ValueError:
                skv_date = _dt.date(y, m, 11)
            if skv_date >= today:
                salary_events.append({
                    'date': skv_date.isoformat(),
                    'type': 'skv',
                    'amount': salary_skv,
                    'label': f'SKV AGI+skatt {skv_date.strftime("%b")}'
                })

        fortnox['salary_events'] = salary_events
        fortnox['salary_nettolon'] = salary_nettolon
        fortnox['salary_skv'] = salary_skv

        # Banksaldo — bokfört saldo från Fortnox SIE4 (konto 1930)
        bank_row = query_all("SELECT balance::numeric(14,2), balance_date::text, updated_at::text FROM bank_balance WHERE company_code='RM' ORDER BY balance_date DESC LIMIT 1")
        if bank_row:
            fortnox['bank_balance'] = float(bank_row[0][0]) if bank_row[0][0] else None
            fortnox['bank_balance_date'] = str(bank_row[0][1]) if bank_row[0][1] else None
            fortnox['bank_balance_updated'] = str(bank_row[0][2]) if bank_row[0][2] else None
        else:
            fortnox['bank_balance'] = None

        # Kundfakturor med labels (for overdue table)
        labeled_ci_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT fortnox_id, customer_name, due_date::text, balance::numeric(14,0), COALESCE(label,'') as label
                FROM fortnox_invoice WHERE company_code='RM' AND balance > 0 AND status != 'cancelled' AND label IS NOT NULL AND label != ''
                ORDER BY label, balance DESC
            ) t
        """)
        fortnox['labeled_customer_invoices'] = labeled_ci_raw

        # Kundfaktura label summor
        fortnox['ci_parkerad_count'] = len([i for i in fortnox['labeled_customer_invoices'] if i.get('label') == 'Parkerad'])
        fortnox['ci_parkerad_total'] = sum(float(i['balance']) for i in fortnox['labeled_customer_invoices'] if i.get('label') == 'Parkerad')
        fortnox['ci_bevakas_count'] = len([i for i in fortnox['labeled_customer_invoices'] if i.get('label') == 'Bevakas'])
        fortnox['ci_bevakas_total'] = sum(float(i['balance']) for i in fortnox['labeled_customer_invoices'] if i.get('label') == 'Bevakas')

        # ── Recurring costs (fasta kostnader) ──
        # Hämta fasta kostnader med payment_interval och sista täckta leverantörsfaktura
        # Exkluderar fakturor med label Parkerad/Bevakas/Tvist (de räknas inte som täckning)
        recurring_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT rc.description, rc.category,
                       ROUND(rc.monthly_amount) as amount,
                       COALESCE(rc.payment_interval, 1) as interval,
                       (SELECT MAX(si.due_date)::text
                        FROM fortnox_supplier_invoice si
                        WHERE si.company_code = 'RM'
                          AND si.status != 'cancelled'
                          AND si.balance > 0
                          AND COALESCE(si.label, '') NOT IN ('Parkerad', 'Bevakas', 'Tvist')
                          AND si.supplier_name ILIKE '%' || LEFT(rc.description,
                              CASE WHEN position(' ' in rc.description) > 0
                                   THEN LEAST(position(' ' in rc.description) - 1, 12)
                                   ELSE LEAST(length(rc.description), 12) END
                          ) || '%'
                       ) as last_invoice_due
                FROM recurring_cost rc
                WHERE rc.company_code='RM' AND rc.is_fixed = true
                ORDER BY rc.monthly_amount DESC
            ) t
        """)
        recurring_items_raw = recurring_raw

        # Beräkna total månadskostnad (för visning)
        recurring_items = [{'description': r['description'], 'category': r['category'], 'amount': r['amount']} for r in recurring_items_raw]
        recurring_total = sum(float(r['amount']) for r in recurring_items)
        fortnox['recurring_items'] = recurring_items
        fortnox['recurring_total_monthly'] = round(recurring_total)

        # Bygg recurring events — per leverantör, bara EFTER sista leverantörsfaktura
        # Respekterar payment_interval (1=mån, 3=kvartal)
        recurring_events = []
        for rc in recurring_items_raw:
            amount_monthly = float(rc['amount'])
            interval = int(rc.get('interval', 1))
            last_due = rc.get('last_invoice_due')

            # Startdatum: månaden efter sista leverantörsfaktura, eller nu
            if last_due:
                ld = _dt.date.fromisoformat(last_due)
                # Första projicerade månad = månaden efter last_due
                start_m = ld.month + 1
                start_y = ld.year
                if start_m > 12:
                    start_m = 1
                    start_y += 1
            else:
                start_m = today.month
                start_y = today.year

            # Event-belopp: monthly_amount * interval (t.ex. 43k/mån * 3 = 128k kvartalsvis)
            event_amount = round(amount_monthly * interval)

            # Generera events 13 månader framåt med rätt intervall
            mo = 0
            while mo < 13:
                m = start_m + mo
                y = start_y + (m - 1) // 12
                m = ((m - 1) % 12) + 1
                ev_date = _dt.date(y, m, 1)
                if ev_date >= today and ev_date <= _dt.date(today.year + 1, today.month, today.day):
                    recurring_events.append({
                        'date': ev_date.isoformat(),
                        'amount': event_amount,
                        'label': f'{rc["description"][:20]} {ev_date.strftime("%b")}',
                        'supplier': rc['description']
                    })
                mo += interval  # Hoppa med rätt intervall

        # Aggregera per datum (kan ha flera leverantörer samma dag)
        from collections import defaultdict
        events_by_date = defaultdict(lambda: {'amount': 0, 'suppliers': []})
        for ev in recurring_events:
            events_by_date[ev['date']]['amount'] += ev['amount']
            events_by_date[ev['date']]['suppliers'].append(ev['label'])

        fortnox['recurring_events'] = [
            {'date': d, 'amount': v['amount'],
             'label': f'Fasta kostnader ({len(v["suppliers"])} poster)'}
            for d, v in sorted(events_by_date.items())
        ]

        # ── Månadsmoms ──
        vat_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT
                    to_char(invoice_date, 'YYYY-MM') as month,
                    COALESCE(SUM(CASE WHEN source='customer' THEN calculated_vat ELSE 0 END), 0)::numeric(14,0) as vat_out,
                    COALESCE(SUM(CASE WHEN source='supplier' THEN calculated_vat ELSE 0 END), 0)::numeric(14,0) as vat_in
                FROM (
                    SELECT invoice_date, total - (total / 1.25) as calculated_vat, 'customer' as source
                    FROM fortnox_invoice WHERE company_code='RM' AND status != 'cancelled' AND NOT is_credit
                    UNION ALL
                    SELECT invoice_date, total - (total / 1.25) as calculated_vat, 'supplier' as source
                    FROM fortnox_supplier_invoice WHERE company_code='RM' AND status != 'cancelled'
                ) sub
                WHERE invoice_date >= date_trunc('month', CURRENT_DATE) - interval '2 months'
                GROUP BY 1
                ORDER BY 1
            ) t
        """)
        vat_months = vat_raw
        # VAT payment: 12th of month after report month. Net = vat_out - vat_in (positive = pay, negative = refund)
        vat_events = []
        for vm in vat_months:
            net = float(vm['vat_out']) - float(vm['vat_in'])
            # Payment date: 12th of next month
            parts = vm['month'].split('-')
            vy, vmo = int(parts[0]), int(parts[1])
            pay_m = vmo + 1
            pay_y = vy
            if pay_m > 12:
                pay_m = 1
                pay_y += 1
            pay_date = _dt.date(pay_y, pay_m, 12)
            if pay_date >= today:
                vat_events.append({
                    'date': pay_date.isoformat(),
                    'net': round(net),
                    'vat_out': round(float(vm['vat_out'])),
                    'vat_in': round(float(vm['vat_in'])),
                    'report_month': vm['month'],
                    'label': f'Moms {vm["month"]}'
                })
        fortnox['vat_events'] = vat_events

        # ── ÄTA-pipeline ──
        ata_raw = safe_json_query("""
            SELECT json_agg(row_to_json(t)) FROM (
                SELECT ata_number, project_name, status,
                       COALESCE(final_amount, estimated_amount, 0)::numeric(14,0) as amount,
                       created_at::date::text as created
                FROM ata_register
                WHERE status IN ('reported', 'approved')
                ORDER BY created_at DESC
                LIMIT 20
            ) t
        """)
        ata_items = ata_raw
        fortnox['ata_pipeline'] = ata_items
        fortnox['ata_reported_total'] = sum(float(a['amount']) for a in ata_items if a.get('status') == 'reported')
        fortnox['ata_approved_total'] = sum(float(a['amount']) for a in ata_items if a.get('status') == 'approved')

        fortnox['has_data'] = True
        fortnox['period_label'] = 'Rullande 12 mån'
        # Fortnox deep-link base URL
        try:
            fnx_cfg = json.load(open('/opt/rm-infra/fortnox-config.json'))
            fortnox['app_id'] = fnx_cfg.get('app_id', '')
        except Exception:
            fortnox['app_id'] = ''
    else:
        fortnox['has_data'] = False
except Exception as e:
    print(f"Fortnox data fetch error: {e}")
    fortnox['has_data'] = False


# -- PLANNER TASKS (unified) --
planner_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT title, assignee_name, bucket_name, plan_name,
           percent_complete, due_date::text, priority,
           CASE
               WHEN due_date < CURRENT_DATE AND percent_complete < 100 THEN 'overdue'
               WHEN due_date = CURRENT_DATE THEN 'today'
               WHEN due_date <= CURRENT_DATE + 7 THEN 'this_week'
               ELSE 'later'
           END as urgency
    FROM planner_task
    WHERE percent_complete < 100 AND company_code = 'RM'
    ORDER BY
        CASE WHEN due_date IS NULL THEN 1 ELSE 0 END,
        due_date ASC,
        priority DESC
) t
""")
try:
    planner = planner_raw
except:
    planner = []

msg_task_stats_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT source, parsed_title as title, parsed_assignee as assignee,
           parsed_project as project, confidence,
           created_at::text
    FROM message_task_log
    WHERE planner_task_id IS NOT NULL AND skipped = false
    ORDER BY created_at DESC LIMIT 5
) t
""")
try:
    msg_task_recent = msg_task_stats_raw
except:
    msg_task_recent = []


# Get Next projects + workorders
next_projects_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT project_id, project_no, project_name, status_code, status_name, customer_id
    FROM next_project ORDER BY status_code, project_no
) t
""")
next_projects = next_projects_raw

next_workorders_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT w.workorder_id, w.project_id, w.project_name, w.project_no, w.workorder_no,
           w.name, w.project_status_code,
           p.status_name
    FROM next_workorder w
    JOIN next_project p ON p.project_id = w.project_id
    ORDER BY w.project_no, w.workorder_no
) t
""")
next_workorders = next_workorders_raw

# Next project economy data (from next_project_economy table)
next_economy_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT ne.project_no, ne.project_name as next_name,
           ne.customer_name, ne.project_manager, ne.status_name as next_status,
           ne.status_code,
           ne.project_type, ne.price_type,
           ne.booked_cost::numeric(14,0) as next_cost,
           ne.booked_revenue::numeric(14,0) as next_revenue,
           ne.booked_hours::numeric(10,1) as next_hours,
           ne.booked_awo_cost::numeric(14,0) as next_awo_cost,
           ne.booked_awo_revenue::numeric(14,0) as next_awo_revenue,
           ne.contribution_margin::numeric(14,0) as next_tb,
           ne.contribution_margin_pct::numeric(6,1) as next_tb_pct,
           ne.budget_cost::numeric(14,0) as next_budget_cost,
           ne.budget_revenue::numeric(14,0) as next_budget_rev,
           ne.budget_contribution_margin::numeric(14,0) as next_budget_tb,
           ne.slp_contribution_margin::numeric(14,0) as next_slp_tb,
           ne.slp_contribution_margin_pct::numeric(6,1) as next_slp_pct,
           ne.earned_revenue::numeric(14,0) as next_earned,
           ne.earned_revenue_not_invoiced::numeric(14,0) as next_earned_not_inv,
           ne.invoiceable::numeric(14,0) as next_invoiceable,
           ne.invoiceable_running::numeric(14,0) as next_invoiceable_running,
           ne.forecast::numeric(14,0) as next_forecast,
           ne.payment_plan_amount::numeric(14,0) as next_payment_plan,
           ne.payment_plan_withheld::numeric(14,0) as next_payment_withheld,
           ne.project_start::text, ne.project_end::text,
           ne.final_inspection_date::text,
           COALESCE(pp.net_revenue,0)::numeric(14,0) as fnx_revenue,
           COALESCE(pp.supplier_costs,0)::numeric(14,0) as fnx_cost,
           COALESCE(pp.tb1,0)::numeric(14,0) as fnx_tb,
           COALESCE(pp.tb1_margin,0)::numeric(6,1) as fnx_tb_pct,
           COALESCE(pp.invoice_count,0) as fnx_inv_count,
           COALESCE(pp.supplier_invoice_count,0) as fnx_si_count,
           COALESCE(pp.project_leader,'') as fnx_leader,
           (ne.booked_cost - COALESCE(pp.supplier_costs,0))::numeric(14,0) as cost_diff,
           ne.synced_at::text as next_synced
    FROM next_project_economy ne
    LEFT JOIN project_profitability pp
        ON pp.project_number = ne.project_no AND pp.company_code = ne.company_code
    WHERE ne.company_code='RM' AND (ne.booked_cost > 0 OR ne.booked_revenue > 0 OR COALESCE(pp.supplier_costs,0) > 0)
    ORDER BY GREATEST(ne.booked_cost, COALESCE(pp.supplier_costs,0)) DESC
) t
""")
next_economy = next_economy_raw


# CRM Audit — latest result
audit_raw = query_one("""SELECT details FROM crm_audit ORDER BY created_at DESC LIMIT 1""")
try:
    audit = audit_raw if isinstance(audit_raw, dict) else {"summary":{"total":0,"high":0,"medium":0,"low":0},"issues":[]}
except:
    audit = {"summary":{"total":0,"high":0,"medium":0,"low":0},"issues":[]}


# TB per affärstyp
tb_type_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT 
        COALESCE(d.deal_type, 'okand') as affarstyp,
        count(DISTINCT u.deal_name)::int as antal_projekt,
        round(sum(COALESCE(u.booked_revenue,0)))::bigint as intakter,
        round(sum(COALESCE(u.booked_cost,0)))::bigint as kostnader,
        round(sum(COALESCE(u.contribution_margin,0)))::bigint as tb,
        CASE WHEN sum(COALESCE(u.booked_revenue,0)) > 0 
            THEN round(sum(COALESCE(u.contribution_margin,0)) / sum(u.booked_revenue) * 100, 1)
            ELSE 0 END::float as tb_pct,
        round(sum(COALESCE(u.earned_revenue_not_invoiced,0)))::bigint as ej_fakturerat
    FROM crm_next_unified u
    LEFT JOIN pipeline_deal d ON u.opportunity_id::text = d.twenty_id
    WHERE u.booked_revenue > 0 OR u.booked_cost > 0
    GROUP BY COALESCE(d.deal_type, 'okand')
    ORDER BY sum(COALESCE(u.contribution_margin,0)) DESC
) t
""")
try:
    tb_per_type = tb_type_raw
except:
    tb_per_type = []

# Pipeline summary per affärstyp
pipe_type_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT 
        deal_type as affarstyp,
        count(*)::int as deals,
        round(sum(estimated_value))::bigint as pipeline_value,
        round(sum(estimated_value * hit_rate / 100))::bigint as viktat_value
    FROM pipeline_deal WHERE company_code='RM'
    GROUP BY deal_type
    ORDER BY sum(estimated_value * hit_rate / 100) DESC
) t
""")
try:
    pipe_per_type = pipe_type_raw
except:
    pipe_per_type = []



# Börsredo KPIs
# Kundkoncentration
cust_conc_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT customer_name, round(sum(total))::bigint as revenue, count(*)::int as invoices
    FROM fortnox_invoice 
    WHERE company_code='RM' AND NOT is_credit AND total > 0
    GROUP BY customer_name ORDER BY sum(total) DESC LIMIT 10
) t
""")
try:
    cust_concentration = cust_conc_raw
except:
    cust_concentration = []

total_rev_raw = query_one("SELECT round(sum(total))::bigint FROM fortnox_invoice WHERE company_code='RM' AND NOT is_credit AND total > 0")
total_revenue = int(total_rev_raw) if total_rev_raw else 0

# Orderstock
orderstock_raw = query_one("SELECT round(sum(estimated_value))::bigint FROM pipeline_deal WHERE company_code='RM' AND stage IN ('kontrakterat','leverans')")
orderstock = int(orderstock_raw) if orderstock_raw else 0

# Fakturerbart ej fakturerat
ej_fakt_raw = query_one("SELECT round(sum(COALESCE(earned_revenue_not_invoiced,0)))::bigint FROM next_project_economy WHERE company_code='RM' AND earned_revenue_not_invoiced > 0")
ej_fakturerat = int(ej_fakt_raw) if ej_fakt_raw else 0

# Orderingång senaste 30 dagar
orderingang_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT to_char(event_date, 'YYYY-MM') as month, count(*)::int as deals, 
           round(sum(COALESCE(estimated_value,0)))::bigint as value
    FROM deal_history WHERE event_type='WON'
    GROUP BY to_char(event_date, 'YYYY-MM') ORDER BY month DESC LIMIT 6
) t
""")
try:
    orderingang = orderingang_raw
except:
    orderingang = []

# Headcount — från rm-config.json, fallback till antal aktiva CRM-användare
try:
    with open('/opt/rm-infra/rm-config.json', 'r') as _cf:
        _config = json.load(_cf)
    headcount = _config.get('headcount', 5)
except:
    headcount = 5


kpi_data = {
    "customer_concentration": cust_concentration,
    "total_revenue": total_revenue,
    "orderstock": orderstock,
    "ej_fakturerat": ej_fakturerat,
    "orderingang": orderingang,
    "headcount": headcount,
    "revenue_per_employee": round(total_revenue / headcount) if total_revenue and headcount else 0
}

# Deal scoring — kör scoring och hämta resultat
import os
os.system("python3 /opt/rm-infra/deal_scoring.py > /dev/null 2>&1")

score_raw = safe_json_query("""
SELECT json_agg(row_to_json(t) ORDER BY t.score DESC) FROM (
    SELECT twenty_id, deal_name, score, score_value, score_stage, score_type, 
           score_hygiene, score_margin, action
    FROM deal_score ORDER BY score DESC
) t
""")
try:
    deal_scores = score_raw
except:
    deal_scores = []



# ── FOKUS DATA ──
fokus_data = {}

# Att fakturera — topp 5 projekt med störst ej-fakturerat
att_fakt_raw = safe_json_query("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT project_no, project_name,
           earned_revenue_not_invoiced::numeric(14,0) as ej_fakturerat,
           project_manager, customer_name, status_name
    FROM next_project_economy
    WHERE company_code='RM' AND earned_revenue_not_invoiced > 50000
    ORDER BY earned_revenue_not_invoiced DESC
    LIMIT 5
) t
""")
try:
    fokus_data['att_fakturera'] = att_fakt_raw
except:
    fokus_data['att_fakturera'] = []

# Scoring topp 3 (deals med action)
score_top_raw = query_one("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT twenty_id, deal_name, score, action
    FROM deal_score
    WHERE action IS NOT NULL AND action != ''
    ORDER BY score DESC
    LIMIT 3
) t
""")
try:
    fokus_data['scoring_top'] = score_top_raw
except:
    fokus_data['scoring_top'] = []

# Planner borttaget från dashboard

# CRM Hygien — deal-detaljer per kategori
hygien = {'utan_varde': [], 'utan_foretag': [], 'utan_leadkalla': []}
try:
    import json as _j
    _uv = safe_json_query("SELECT json_agg(json_build_object('name', name, 'id', twenty_id, 'stage', stage)) FROM pipeline_deal WHERE company_code='RM' AND stage IN ('kontrakterat','leverans','forhandling','offert_skickad') AND (estimated_value IS NULL OR estimated_value = 0)")
    hygien['utan_varde'] = _uv if _uv else []
    _uf = safe_json_query("SELECT json_agg(json_build_object('name', name, 'id', twenty_id, 'stage', stage)) FROM pipeline_deal WHERE company_code='RM' AND (customer_name IS NULL OR customer_name = '') AND stage NOT IN ('fakturerat')")
    hygien['utan_foretag'] = _uf if _uf else []
    _ul = safe_json_query("SELECT json_agg(json_build_object('name', name, 'id', twenty_id, 'stage', stage)) FROM pipeline_deal WHERE company_code='RM' AND (lead_source IS NULL OR lead_source = '') AND stage NOT IN ('fakturerat')")
    hygien['utan_leadkalla'] = _ul if _ul else []
except Exception as e:
    print(f"  Hygien error: {e}")
fokus_data['crm_hygien'] = hygien

# Planner tasks for Fokus
fokus_data['planner_tasks'] = planner[:10] if planner else []
fokus_data['planner_overdue'] = len([t for t in (planner or []) if t.get('urgency') == 'overdue'])
fokus_data['planner_today'] = len([t for t in (planner or []) if t.get('urgency') == 'today'])
fokus_data['planner_total'] = len(planner or [])
fokus_data['msg_task_recent'] = msg_task_recent


# ═══ SCALING UP: Rocks, Scorecard, Goals ═══
from datetime import datetime as _dt
_m = _dt.now().month
_qn = (_m - 1) // 3 + 1
current_quarter = f"{_dt.now().year}-Q{_qn}"

rocks = safe_json_query("""
    SELECT json_agg(row_to_json(t)) FROM (
        SELECT r.id, r.quarter, r.title, r.owner, r.role, r.status, r.progress,
               r.due_date::text, r.notes, r.parent_goal_id,
               g.title as goal_title
        FROM quarterly_rock r
        LEFT JOIN annual_goal g ON r.parent_goal_id = g.id
        WHERE r.company_code='RM'
        ORDER BY r.quarter DESC, r.status = 'done', r.due_date NULLS LAST
    ) t
""")
print(f"  Rocks: {len(rocks)}")

scorecard = safe_json_query("""
    SELECT json_agg(row_to_json(t)) FROM (
        SELECT s.id, s.metric_name, s.role, s.target_value, s.unit, s.frequency,
               s.owner, s.is_green_above, s.sort_order,
               (SELECT json_agg(json_build_object('period', e.period, 'value', e.actual_value, 'notes', e.notes)
                ORDER BY e.period DESC)
                FROM scorecard_entry e WHERE e.target_id = s.id
               ) as entries
        FROM scorecard_target s
        WHERE s.active=true AND s.company_code='RM'
        ORDER BY s.role, s.sort_order
    ) t
""")
print(f"  Scorecard targets: {len(scorecard)}")

annual_goals = safe_json_query("""
    SELECT json_agg(row_to_json(t)) FROM (
        SELECT g.id, g.year, g.title, g.category, g.target_value, g.current_value, g.unit, g.owner,
               (SELECT count(*) FROM quarterly_rock r WHERE r.parent_goal_id = g.id) as rock_count,
               (SELECT count(*) FROM quarterly_rock r WHERE r.parent_goal_id = g.id AND r.status='done') as rocks_done
        FROM annual_goal g
        WHERE g.company_code='RM' AND g.year = EXTRACT(YEAR FROM CURRENT_DATE)::int
        ORDER BY g.category, g.id
    ) t
""")
print(f"  Annual goals: {len(annual_goals)}")


# Read template
with open("/opt/rm-infra/dashboard-template.html", "r") as f:
    html = f.read()

# Inject data
html = re.sub(
    r'const DEALS = \[[\s\S]*?\];',
    f'const DEALS = {json.dumps(deals, ensure_ascii=False)};',
    html
)
html = re.sub(
    r'const HITRATE_MATRIX = \{[\s\S]*?\n\};',
    f'const HITRATE_MATRIX = {json.dumps(hitrate, ensure_ascii=False)};',
    html
)
html = re.sub(
    r'const FORTNOX = \{[\s\S]*?\n\};',
    f'const FORTNOX = {json.dumps(fortnox, ensure_ascii=False)};',
    html
)


html = re.sub(
    r'const PLANNER = \[[\s\S]*?\];',
    f'const PLANNER = {json.dumps(planner, ensure_ascii=False)};',
    html
)


html = re.sub(
    r'const NEXT_PROJECTS = \[[\s\S]*?\];',
    f'const NEXT_PROJECTS = {json.dumps(next_projects, ensure_ascii=False)};',
    html
)
html = re.sub(
    r'const NEXT_WORKORDERS = \[[\s\S]*?\];',
    f'const NEXT_WORKORDERS = {json.dumps(next_workorders, ensure_ascii=False)};',
    html
)

html = re.sub(
    r'const NEXT_ECONOMY = \[[\s\S]*?\];',
    f'const NEXT_ECONOMY = {json.dumps(next_economy, ensure_ascii=False)};',
    html
)

# CRM + Next unified data
try:
    unified_raw = query_one("""
    SELECT json_agg(row_to_json(t)) FROM (
        SELECT deal_name, crm_stage, next_project_no, next_id, opportunity_id::text, company_name,
               contact_name, contact_phone, contact_email,
               next_name, next_status, next_customer,
               budget_revenue::numeric(14,0), budget_cost::numeric(14,0),
               budget_contribution_margin::numeric(14,0), budget_contribution_margin_pct::numeric(6,1),
               booked_revenue::numeric(14,0), booked_cost::numeric(14,0),
               contribution_margin::numeric(14,0), contribution_margin_pct::numeric(6,1),
               forecast::numeric(14,0),
               earned_revenue::numeric(14,0), earned_revenue_not_invoiced::numeric(14,0),
               invoiceable::numeric(14,0),
               project_start::text, project_end::text,
               project_manager,
               final_inspection_date::text
        FROM crm_next_unified
        ORDER BY deal_name
    ) t
    """)
    crm_next_unified = unified_raw
except Exception as e:
    print(f"CRM_NEXT_UNIFIED fetch error: {e}")
    crm_next_unified = []

html = re.sub(
    r'const CRM_NEXT_UNIFIED = \[[\s\S]*?\];',
    f'const CRM_NEXT_UNIFIED = {json.dumps(crm_next_unified, ensure_ascii=False)};',
    html
)


html = re.sub(
    r'const CRM_AUDIT = \{[^;]*\};',
    f'const CRM_AUDIT = {json.dumps(audit, ensure_ascii=False)};',
    html
)


html = re.sub(
    r'const TB_PER_TYPE = \[[^;]*\];',
    f'const TB_PER_TYPE = {json.dumps(tb_per_type, ensure_ascii=False)};',
    html
)
html = re.sub(
    r'const PIPE_PER_TYPE = \[[^;]*\];',
    f'const PIPE_PER_TYPE = {json.dumps(pipe_per_type, ensure_ascii=False)};',
    html
)


html = re.sub(
    r'const DEAL_SCORES = \[[^;]*\];',
    f'const DEAL_SCORES = {json.dumps(deal_scores, ensure_ascii=False)};',
    html
)


html = re.sub(
    r'const KPI_DATA = \{[^;]*\};',
    f'const KPI_DATA = {json.dumps(kpi_data, ensure_ascii=False)};',
    html
)



html = re.sub(
    r'const FOKUS_DATA = \{[^;]*\};',
    f'const FOKUS_DATA = {json.dumps(fokus_data, ensure_ascii=False)};',
    html
)


# Scaling Up data
html = re.sub(
    r'const ROCKS = \[\];',
    f'const ROCKS = {json.dumps(rocks, ensure_ascii=False, default=str)};',
    html
)
html = re.sub(
    r'const SCORECARD = \[\];',
    f'const SCORECARD = {json.dumps(scorecard, ensure_ascii=False, default=str)};',
    html
)
html = re.sub(
    r'const ANNUAL_GOALS = \[\];',
    f'const ANNUAL_GOALS = {json.dumps(annual_goals, ensure_ascii=False, default=str)};',
    html
)
html = re.sub(
    r'const CURRENT_QUARTER = \'\'\;',
    f"const CURRENT_QUARTER = '{current_quarter}';",
    html
)

# Write output
out = "/opt/rm-infra/www/index.html"
with open(out, "w") as f:
    f.write(html)

print(f"{datetime.now()}: Dashboard generated — {len(deals)} deals, {len(next_projects)} next projects, {len(next_economy)} next economy, {len(crm_next_unified)} unified, Fortnox: {fortnox.get('has_data', False)}")
if fortnox.get('has_data'):
    print(f"  Overdue real: {fortnox.get('overdue_real',0)} SEK, labeled: {fortnox.get('overdue_labeled',0)} SEK")
    print(f"  Overdue invoices: {len(fortnox.get('overdue_invoices',[]))}")
