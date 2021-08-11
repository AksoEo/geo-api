import fetch from 'node-fetch';

// Potentially very slow
export async function getSubClasses (parentClass) {
	const url = new URL('https://query.wikidata.org/sparql')
	url.searchParams.set('query', `SELECT ?s WHERE { ?s wdt:P279+ wd:${parentClass} . }`);
	let res = await fetch(url, {
		method: 'GET',
		headers: {
			Accept: 'application/sparql-results+json;charset=utf-8'
		}
	});
	res = await res.json();
	return res.results.bindings.map(function (entity) {
		const uri = new URL(entity.s.value);
		const bits = uri.pathname.split('/');
		return bits[bits.length - 1];
	});
}
