const readline = require("readline"),
  path = require("path"),
  url = require("url"),
  _ = require("lodash"),
  http = require("http"),
  https = require("https"),
  Excel = require("exceljs"),
  utilities = require('@big-data-processor/utilities'),
  fse = utilities.fse,
  globbyAsync = utilities.globbyAsync,
  pathMapping = utilities.pathMapping,
  memHumanize = utilities.humanizeMemory

module.exports = function(nunjuckEnv) {
  nunjuckEnv.addFilter("log", function(input) {
    console.log(input);
    return input;
  });
  nunjuckEnv.addFilter("humanizeMemory", function(bytes, si, decimals) {
    return memHumanize(bytes, si, decimals);
  });
  nunjuckEnv.addFilter("empty", function() { return; });
  nunjuckEnv.addFilter("parsePath", function(filepath) { return path.parse(filepath); });
  nunjuckEnv.addFilter("parseURL", function(theURL) {
    return URL ? new URL(theURL) : url.parse(theURL)
  });
  nunjuckEnv.addFilter("httpGET", function(url, targetFilePath, cb) {
    if (!cb) {
      cb = targetFilePath;
      targetFilePath = undefined;
    }
    const protocol = (new URL(url).protocol == 'https:' ? https : http);
    protocol.get(url, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        cb(`httpGet error: 'Request Failed.\n'` + `Status Code: ${res.statusCode}`, []);
        return;
      }
      let rawData = '';
      res.on('data', (chunk) => { rawData += chunk; });
      res.on('end', () => {
        if (targetFilePath) {
          fse.writeFile(targetFilePath, rawData, function(err) {
            return err ? cb(err, []) : cb(null, targetFilePath);
          });
        }else {
          cb(null, rawData.toString());
        }
      });
    }).on('error', err => cb(`httpGet error: ${err.message}`, []));
  }, true);
  nunjuckEnv.addFilter(
    "ensureDir",
    function(dirPath, cb) {
      fse.ensureDir(dirPath, err => err ? cb(err, []) : cb(null, dirPath));
    },
    true
  );
  nunjuckEnv.addFilter(
    "emptyDir",
    function(dirPath, cb) {
      fse.emptyDir(dirPath, function(err) {
        return err ? cb(err, []) : cb(null, dirPath);
      });
    },
    true
  );
  nunjuckEnv.addFilter(
    "eval",
    function(data, stringToEval) {
      return eval(stringToEval);
    }
  );
  nunjuckEnv.addFilter(
    "listFromFileGlob",
    function(folderpath, patterns, options, cb) {
      if (typeof options === 'function') {
        cb = options;
        options = undefined;
      }
      globbyAsync(folderpath.replace(/\\ /g, " "), patterns, options)
        .then(files => cb(null, files))
        .catch(err => cb(err, []));
    },
    true
  );

  nunjuckEnv.addFilter(
    "fileStats",
    function(arrayOfFiles, cb) {
      if (!Array.isArray(arrayOfFiles)) {
        arrayOfFiles = [arrayOfFiles];
      }
      const fileStatsPromises = arrayOfFiles.map(filePath => {
        return fse.stat(filePath);
      });
      Promise.all(fileStatsPromises).then((fileStats) => {
        cb(null, fileStats);
      }).catch((errs) => {
        cb(errs, []);
      });
    },
    true
  );
  nunjuckEnv.addFilter(
    "listFromExcel",
    function(excelpath, sheet, hasHeader, returnType, cb) {
      const returnList = [];
      const workbook = new Excel.Workbook();
      workbook.xlsx
        .readFile(excelpath.replace(/\\ /g, " "))
        .then(() => {
          const worksheet = workbook.getWorksheet(sheet);
          let header = [];
          worksheet.eachRow((row, rowNumber) => {
            if (hasHeader && rowNumber == 1) {
              header = row.values;
            } else {
              if (hasHeader && returnType === "object") {
                const eachObj = {};
                header.forEach((h, i) => {
                  eachObj[h] = row.values[i];
                });
                returnList.push(eachObj);
              } else {
                returnList.push(row.values.slice(1));
              }
            }
          });
          cb(null, returnList);
        })
        .catch(err => {
          cb(err, []);
        });
    },
    true
  );

  nunjuckEnv.addFilter(
    "listFromCSV",
    function(csvPath, hasHeader, returnType, cb) {
      const returnList = [];
      const workbook = new Excel.Workbook();
      workbook.csv
        .readFile(csvPath.replace(/\\ /g, " "))
        .then(worksheet => {
          let header = [];
          worksheet.eachRow((row, rowNumber) => {
            if (hasHeader && rowNumber == 1) {
              header = row.values;
            } else {
              if (hasHeader && returnType === "object") {
                const eachObj = {};
                header.forEach((h, i) => {
                  eachObj[h] = row.values[i];
                });
                returnList.push(eachObj);
              } else {
                returnList.push(row.values.slice(1));
              }
            }
          });
          cb(null, returnList);
        })
        .catch(err => {
          cb(err, []);
        });
    },
    true
  );

  nunjuckEnv.addFilter(
    "listFromText",
    function(filePath, sep, hasHeader, returnType, cb) {
      const returnList = [];
      const fileStream = fse.createReadStream(filePath);
      fileStream.on("error", err => {
        cb(err, []);
      });
      const rl = readline.createInterface({ input: fileStream });
      rl.on("line", line => {
        returnList.push(sep ? line.split(sep) : line);
      });
      rl.on("close", () => {
        let headers = [];
        if (hasHeader) {
          headers = returnList.splice(0, 1);
        }
        if (returnType !== 'object' || !hasHeader) {
          return cb(null, returnList);
        }
        return cb(null, returnList.map((data) => {
          const eachObj = {};
          headers.forEach((h, i) => { eachObj[h] = data[i]; });
          return eachObj;
        }));
      });
    },
    true
  );

  nunjuckEnv.addFilter("pathMapping", function(oldPath, refPath, inDockerPathRef) {
    if (Array.isArray(oldPath)) {
      return oldPath.map((opath) => pathMapping(opath, refPath, inDockerPathRef));
    }else {
      if (oldPath === undefined) {
        throw 'You used an undefined value to do pathMapping.';
      } else if (oldPath === null) {
        throw 'You used a null value to do pathMapping.';
      } else {
        return pathMapping(oldPath, refPath, inDockerPathRef);
      }
    }
  });

  nunjuckEnv.addFilter("prefixArray", function(theArray, thePrefix) {
    if (Array.isArray(theArray)) {
      return theArray.map(arrayData => {
        return thePrefix + arrayData;
      });
    } else {
      return theArray;
    }
  });
  nunjuckEnv.addFilter("suffixArray", function(theArray, theSuffix) {
    if (Array.isArray(theArray)) {
      return theArray.map(arrayData => {
        return arrayData + theSuffix;
      });
    } else {
      return theArray;
    }
  });
  nunjuckEnv.addFilter("split", function(str, splitter) {
    return str.split(splitter);
  });
  nunjuckEnv.addFilter("prefix", function(data, thePrefix) {
    if (Array.isArray(data)) {
      return data.map(arrayData => {
        return String(thePrefix) + String(arrayData);
      });
    } else {
      return String(thePrefix) + String(data);
    }
  });
  nunjuckEnv.addFilter("suffix", function(data, theSuffix) {
    if (Array.isArray(data)) {
      return data.map(arrayData => {
        return String(arrayData) + theSuffix;
      });
    } else {
      return String(data) + String(theSuffix);
    }
  });
  nunjuckEnv.addFilter("zipAsStrings", function(joiner) {
    const zippedArray = _.zip(arguments);
    const returnArray = [];
    joiner = joiner | "";
    for (const element of zippedArray) {
      returnArray.push(element.join(joiner));
    }
    return returnArray;
  });

  nunjuckEnv.addFilter("_", function() {
    const theLodashFunc = _[arguments[1]];
    const theArgs = [arguments[0]];
    for (let i = 2; i < arguments.length; i++) {
      theArgs.push(arguments[i]);
    }
    return theLodashFunc.apply(this, theArgs);
  });
  nunjuckEnv.addFilter("regTest", function(str, regStr, flags) {
    return (new RegExp(regStr, flags)).test(str);
  });
  nunjuckEnv.addFilter("regMatch", function(str, regStr, flags) {
    return str.match(new RegExp(regStr, flags));
  });
  nunjuckEnv.addFilter('encodeString', function(str, encodingFrom, encodingTo) {
    return Buffer.from(str, encodingFrom).toString(encodingTo);
  });
};
