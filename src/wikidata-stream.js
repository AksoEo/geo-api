import get from 'simple-get';
import bz2 from 'unbzip2-stream';
import stream from 'stream';
import chunkingStreams from 'chunking-streams';

const wikidataDumpsURL = 'https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2';

class JSONParseStream extends stream.Transform {
	constructor (options) {
		super({
			...options,
			objectMode: true
		});
	}

	_transform (chunk, encoding, callback) {
		let str = chunk.toString();
		str = str.substring(str, str.length - 1); // remove ,
		if (!str.length) { return callback(null, null); }
		let obj;
		try {
			obj = JSON.parse(str);
		} catch (e) {
			return callback(null, null);
		}
		callback(null, obj);

	}
}

export async function getWikidataStream () {
	const httpsPipe =  await new Promise(function (resolve, reject) {
		get(wikidataDumpsURL, function (err, res) {
			if (err) { return reject(err); }
			resolve(res);
		});
	});
	return httpsPipe
		.pipe(bz2())
		.pipe(
			new chunkingStreams.SeparatorChunker({
				separator: '\n'
			})
		)
		.pipe(new JSONParseStream());
}
