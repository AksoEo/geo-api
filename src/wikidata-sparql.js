import fetch from 'node-fetch';

// Everything in this file is potentially very slow

const baseURL = 'https://query.wikidata.org/sparql';

export async function getSubClasses (parentClass) {
	const url = new URL(baseURL)
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
