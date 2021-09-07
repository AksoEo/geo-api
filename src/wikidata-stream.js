import get from 'simple-get';
import stream from 'stream';
import chunkingStreams from 'chunking-streams';
import Meter from 'stream-meter';
import bytes from 'bytes';
import { DateTime, Duration } from 'luxon';
import { spawn } from 'child_process';

const wikidataDumpsURL = 'https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2';

const byteFormatObj = {
	thousandsSeparator: ',',
	unitSeparator: ' ',
	fixedDecimals: true
};

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

class UnBzip2 extends stream.Duplex {
	constructor () {
		super();
		this.unbz2 = spawn('bunzip2');
		this.unbz2.stdout.on('data', data => {
			this.push(data);
		});
	}

	_write (chunk, encoding, callback) {
		const maxReadableLength = 2 * (2 ** 20);
		this.unbz2.stdin.write(chunk);

		if (this.readableLength > maxReadableLength) {
			const waitForSafeWriteInterval = setInterval(function () {
				if (this.readableLength > maxReadableLength) { return; }
				clearInterval(waitForSafeWriteInterval);
				callback();
			}, 100);
		} else { callback(); }
	}

	_read (size) {}
}

export async function getWikidataStream (noMeter = false) {
	const httpsPipe =  await new Promise(function (resolve, reject) {
		get(wikidataDumpsURL, function (err, res) {
			if (err) { return reject(err); }
			resolve(res);
		});
	});
	const size = parseInt(httpsPipe.headers['content-length'], 10);
	const meter = Meter();
	const startTime = DateTime.now();

	if (!noMeter) {
		console.log(`... total of ${bytes.format(size, byteFormatObj)}`)
	}

	let prevBytes = 0;
	let prevTime = startTime;
	const meterInterval = setInterval(() => {
		if (noMeter) { return; }
		const bytesFormat = bytes.format(meter.bytes, byteFormatObj);
		const percent = `${(meter.bytes / size * 100).toFixed(4)}%`;
		const timeNow = DateTime.now();
		const deltaTime = timeNow.diff(startTime, [ 'hours', 'minutes', 'seconds' ]);
		const deltaTimeFormat = deltaTime.toFormat('hh:mm:ss');
		const deltaBytes = meter.bytes - prevBytes;
		const deltaTimeSeconds = (timeNow.toSeconds() - prevTime.toSeconds());
		const speed = deltaBytes / deltaTimeSeconds;
		const speedFormat = bytes.format(speed, byteFormatObj) + '/s';
		const deltaTimeTotalSeconds = (timeNow.toSeconds() - startTime.toSeconds());
		const speedTotal = meter.bytes / deltaTimeTotalSeconds;
		const bytesLeft = size - meter.bytes;
		const secondsLeft = bytesLeft / speedTotal;
		const timeLeft = Duration.fromObject({ seconds: secondsLeft }).toFormat('dd:hh:mm:ss');
		console.log(`... ${bytesFormat} (${percent}) down, ${deltaTimeFormat} passed, ${speedFormat}, done in ${timeLeft}`)
		prevBytes = meter.bytes;
		prevTime = timeNow;
	}, 2000);

	const stream = httpsPipe
		.pipe(meter)
		.pipe(new UnBzip2())
		// chunking streams is naïve and does not care for multi-byte chars in utf-8
		// however, we are only receiving ASCII as all the unicode is encoded in the JSON
		// as such this does not at the current point in time present any problem
		.pipe(
			new chunkingStreams.SeparatorChunker({
				separator: '\n'
			})
		)
		.pipe(new JSONParseStream());

	stream.on('finish', function () {
		clearInterval(meterInterval);
	});

	return stream;
}
