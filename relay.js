/**
 * RM Prospekteringsagent — Combify Relay Script
 * Körs som bookmarklet på map.combify.com
 * Hämtar bygglov + markanvisningar → öppnar receiver-sidan med data
 */
(function() {
    'use strict';

    const SERVER = 'https://rm-api.161-35-79-92.nip.io';
    const GQL = 'https://wktutp4vpbezrbwtwea4c6qd7m.appsync-api.eu-west-1.amazonaws.com/graphql';
    const CLIENT_ID = '4spuf8r6iel759h8li3g1tbcvi';

    const STOCKHOLM = new Set([
        'Stockholm','Solna','Sundbyberg','Järfälla','Upplands-Bro',
        'Sigtuna','Vallentuna','Norrtälje','Täby','Danderyd',
        'Lidingö','Nacka','Haninge','Huddinge','Botkyrka',
        'Södertälje','Nykvarn','Salem','Ekerö','Värmdö',
        'Tyresö','Nynäshamn','Upplands Väsby','Vaxholm',
    ]);

    const RELEVANT = new Set([
        'residential','infrastructure_technical','industrial',
        'office','community_function','educational',
        'sports_recreation','healthcare','retail',
    ]);

    const PROC_SV = {new_building:'Nybyggnad',addition:'Tillbyggnad',modification:'Ändring',demolition:'Rivning'};
    const LABEL_SV = {residential:'Bostad',industrial:'Industri',office:'Kontor',retail:'Handel',educational:'Utbildning',healthcare:'Vård',infrastructure_technical:'Infrastruktur',community_function:'Samhällsfunktion',sports_recreation:'Sport/Fritid'};

    // --- UI ---
    let panel = document.getElementById('rm-relay-panel');
    if (panel) panel.remove();

    panel = document.createElement('div');
    panel.id = 'rm-relay-panel';
    panel.style.cssText = 'position:fixed;top:20px;right:20px;width:380px;background:#0d1117;border:1px solid #30363d;border-radius:10px;z-index:999999;font-family:-apple-system,sans-serif;color:#c9d1d9;box-shadow:0 8px 32px rgba(0,0,0,0.6);';
    panel.innerHTML = `
        <div style="padding:14px 18px;border-bottom:1px solid #30363d;display:flex;justify-content:space-between;align-items:center">
            <span style="font-weight:600;color:#58a6ff;font-size:.95em">RM Prospekteringsagent</span>
            <span id="rm-close" style="cursor:pointer;color:#8b949e;font-size:1.2em">&times;</span>
        </div>
        <div id="rm-log" style="padding:12px 18px;font-size:.8em;font-family:'SF Mono',monospace;max-height:300px;overflow-y:auto;line-height:1.7"></div>
    `;
    document.body.appendChild(panel);
    document.getElementById('rm-close').onclick = () => panel.remove();

    const logEl = document.getElementById('rm-log');
    function log(msg, color) {
        const c = {ok:'#3fb950',warn:'#d29922',err:'#f85149',info:'#8b949e'}[color||'info'];
        logEl.innerHTML += `<div style="color:${c}">${new Date().toLocaleTimeString('sv-SE')} ${msg}</div>`;
        logEl.scrollTop = logEl.scrollHeight;
    }

    // --- Token ---
    function getToken() {
        const keys = Object.keys(localStorage).filter(k => k.endsWith('.idToken'));
        if (keys.length > 0) return localStorage.getItem(keys[0]);
        return null;
    }

    // --- GraphQL ---
    async function gql(query, variables) {
        const token = getToken();
        if (!token) throw new Error('Ingen token — logga in på Combify');
        const r = await fetch(GQL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': token },
            body: JSON.stringify({ query, variables }),
        });
        if (!r.ok) throw new Error('Combify HTTP ' + r.status);
        const d = await r.json();
        if (d.errors && !d.data) throw new Error(d.errors[0].message);
        return d;
    }

    function estValue(area, proc, labels) {
        if (!area) return 500000;
        const rates = {new_building:25000,addition:18000,modification:12000,demolition:3000};
        let r = rates[proc] || 15000;
        if (labels.some(l => l==='residential'||l==='office')) r *= 1.2;
        if (labels.some(l => l==='industrial'||l==='storage_warehouse')) r *= 0.7;
        return Math.max(500000, Math.min(area * r, 50000000));
    }

    // --- Permits ---
    async function fetchPermits() {
        const data = await gql(`query search($input: SearchFiltersInput!) {
            search(input: $input) {
                ... on PermitSearchResponse {
                    items { permit_id sublocality block_unit municipality_name decision_date procedure sub_categories { area_m2 budget_sek } labels { label } }
                    total
                }
            }
        }`, {
            input: { Permit: { search: true, sort: { key: 'date', order: 'desc' } } }
        });

        let resp = data?.data?.search;
        if (Array.isArray(resp)) resp = resp.find(r => r?.items) || {};
        const items = resp?.items || [];
        log(`${resp?.total || 0} totala bygglov, ${items.length} hämtade`, 'info');

        const leads = [];
        for (const p of items) {
            if (!STOCKHOLM.has(p.municipality_name || '')) continue;
            const labels = (p.labels||[]).map(l=>l.label);
            if (labels.length && !labels.some(l=>RELEVANT.has(l))) continue;

            const proc = p.procedure || '';
            let mainLabel = '';
            for (const l of labels) { if (LABEL_SV[l]) { mainLabel = LABEL_SV[l]; break; } }
            if (!mainLabel) mainLabel = 'Byggprojekt';

            let name = `${PROC_SV[proc]||proc} ${mainLabel}`;
            if (p.sublocality) name += ` Kv ${p.sublocality.charAt(0).toUpperCase()+p.sublocality.slice(1).toLowerCase()}`;
            if (p.block_unit) name += ` ${p.block_unit}`;
            name += ` — ${p.municipality_name}`;

            const sc = p.sub_categories || {};
            const value = (sc.budget_sek > 0) ? sc.budget_sek : estValue(sc.area_m2, proc, labels);
            leads.push({ name, value });
        }
        return { leads, fetched: items.length };
    }

    // --- Tenders ---
    async function fetchTenders() {
        const data = await gql(`query search($input: SearchFiltersInput!) {
            search(input: $input) {
                ... on TenderSearchResponse {
                    items { project_id name municipality_name sub_categories { units } }
                    total
                }
            }
        }`, { input: { Tender: { search: true } } });

        let resp = data?.data?.search;
        if (Array.isArray(resp)) resp = resp.find(r => r?.items) || {};
        const items = resp?.items || [];
        log(`${items.length} markanvisningar hämtade`, 'info');

        const leads = [];
        for (const t of items) {
            if (!STOCKHOLM.has(t.municipality_name || '')) continue;
            const units = t.sub_categories?.units || 0;
            const value = Math.min(Math.max(5000000, units*3000000)||10000000, 50000000);
            leads.push({ name: `Markanvisning: ${t.name||'Okänd'} — ${t.municipality_name}`, value });
        }
        return { leads, fetched: items.length };
    }

    // --- Run ---
    async function run() {
        try {
            log('Startar prospektering...', 'ok');

            const p = await fetchPermits();
            log(`${p.leads.length} relevanta bygglov i Stockholm`, 'ok');

            const t = await fetchTenders();
            log(`${t.leads.length} relevanta markanvisningar`, 'ok');

            const all = [...p.leads, ...t.leads];

            if (all.length === 0) {
                log('Inga nya relevanta leads', 'warn');
                return;
            }

            log(`${all.length} leads totalt — skickar till RM-servern...`, 'info');

            // Post directly to HTTPS endpoint (no mixed content issue)
            const resp = await fetch(SERVER + '/api/leads', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Api-Key': 'rm-combify-2026' },
                body: JSON.stringify({ leads: all }),
            });
            if (!resp.ok) throw new Error('Server HTTP ' + resp.status);
            const result = await resp.json();

            log(`Klart — ${result.created} nya leads, ${result.duplicates} dubbletter`, 'ok');
            if (result.created > 0) log('Dashboard uppdateras automatiskt', 'ok');

        } catch(e) {
            log('FEL: ' + e.message, 'err');
            if (e.message.includes('token')) {
                log('Ladda om Combify-sidan och forsok igen', 'warn');
            }
        }
    }

    run();
})();
