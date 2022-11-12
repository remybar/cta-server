
/**
 * Partition an array in 2 parts: the items which
 * satisfy the filter function, and the others.
 * @param {*} data array to partition
 * @param {*} filter a filter function
 */
const partition = (data, filter) => {
    return data.reduce(
        (r, o) => {
            r[filter(o) ? 0 : 1].push(o);
            return r;
        },
        [[], []]
    );
}

const splitInChunks = (rows, size) => {
    let chunks = [];
    
    while (rows.length) {
        chunks.push(rows.splice(0, size));
    }
    return chunks;
}

module.exports = {partition, splitInChunks};