const { errors, versioning } = require('arsenal');
const async = require('async');

const metadata = require('../../../metadata/wrapper');
const { config } = require('../../../Config');

const versionIdUtils = versioning.VersionID;
// Use Arsenal function to generate a version ID used internally by metadata
// for null versions that are created before bucket versioning is configured
const nonVersionedObjId =
    versionIdUtils.getInfVid(config.replicationGroupId);

/** decodedVidResult - decode the version id from a query object
 * @param {object} [reqQuery] - request query object
 * @param {string} [reqQuery.versionId] - version ID sent in request query
 * @return {(Error|string|undefined)} - return Invalid Argument if decryption
 * fails due to improper format, otherwise undefined or the decoded version id
 */
function decodeVersionId(reqQuery) {
    if (!reqQuery || !reqQuery.versionId) {
        return undefined;
    }
    let versionId = reqQuery.versionId;
    if (versionId === 'null') {
        return versionId;
    }
    versionId = versionIdUtils.decode(versionId);
    if (versionId instanceof Error) {
        return errors.InvalidArgument
            .customizeDescription('Invalid version id specified');
    }
    return versionId;
}

/** getVersionIdResHeader - return encrypted version ID if appropriate
 * @param {object} [verCfg] - bucket versioning configuration
 * @param {object} objectMD - object metadata
 * @return {(string|undefined)} - undefined or encrypted version ID
 * (if not 'null')
 */
function getVersionIdResHeader(verCfg, objectMD) {
    if (verCfg) {
        if (objectMD.isNull || !objectMD.versionId) {
            return 'null';
        }
        return versionIdUtils.encode(objectMD.versionId,
                                     config.versionIdEncodingType);
    }
    return undefined;
}

/**
 * Checks for versionId in request query and returns error if it is there
 * @param {object} query - request query
 * @return {(Error|undefined)} - customized InvalidArgument error or undefined
 */
function checkQueryVersionId(query) {
    if (query && query.versionId !== undefined) {
        const customMsg = 'This operation does not accept a version-id.';
        return errors.InvalidArgument.customizeDescription(customMsg);
    }
    return undefined;
}

function _storeNullVersionMD(bucketName, objKey, objMD, options, log, cb) {
    metadata.putObjectMD(bucketName, objKey, objMD, options, log, err => {
        if (err) {
            log.debug('error from metadata storing null version as new version',
            { error: err });
        }
        cb(err, options);
    });
}

/** get location of null version data for deletion
* @param {string} bucketName - name of bucket
* @param {string} objKey - name of object key
* @param {object} options - metadata options for getting object MD
* @param {string} options.versionId - version to get from metadata
* @param {object} mst - info about the master version
* @param {string} mst.versionId - the master version's version id
* @param {RequestLogger} log - logger instanceof
* @param {function} cb - callback
* @return {undefined} - and call callback with (err, dataToDelete)
*/
function _getNullVersionsToDelete(bucketName, objKey, options, mst, log, cb) {
    if (options.versionId === mst.versionId) {
        // no need to get delete location, we already have the master's metadata
        const dataToDelete = mst.objLocation;
        return process.nextTick(cb, null, dataToDelete);
    }
    return metadata.getObjectMD(bucketName, objKey, options, log,
        (err, versionMD) => {
            if (err) {
                log.debug('err from metadata getting specified version', {
                    error: err,
                    method: '_getNullVersionsToDelete',
                });
                return cb(err);
            }
            if (!versionMD.location) {
                return cb();
            }
            const dataToDelete = Array.isArray(versionMD.location) ?
                versionMD.location : [versionMD.location];
            return cb(null, dataToDelete);
        });
}

function _deleteNullVersionMD(bucketName, objKey, options, mst, log, cb) {
    return _getNullVersionsToDelete(bucketName, objKey, options, mst, log,
        (err, nullDataToDelete) => {
            if (err) {
                log.warn('could not find null version metadata', {
                    error: err,
                    method: '_deleteNullVersionMD',
                });
                return cb(err);
            }
            return metadata.deleteObjectMD(bucketName, objKey, options, log,
                err => {
                    if (err) {
                        log.warn('metadata error deleting null version',
                        { error: err, method: '_deleteNullVersionMD' });
                        return cb(err);
                    }
                    return cb(null, nullDataToDelete);
                });
        });
}

/**
 * Process state from the master version of an object and the bucket
 * versioning configuration, return a set of options objects
 *
 * @param {object} mst - state of master version, as returned by
 * getMasterState()
 * @param {string} vstat - bucket versioning status: 'Enabled' or 'Suspended'
 * @param {boolean} nullVersionCompatMode - if true, behaves in null
 * version compatibility mode and return appropriate values: this mode
 * does not attempt to create null keys but create null versioned keys
 * instead
 *
 * @return {object} result object with the following attributes:
 * - {object} options: versioning-related options to pass to the
     services.metadataStoreObject() call
 * - {object} [options.extraMD]: extra attributes to set in object metadata
 * - {string} [nullVersionId]: null version key to create, if needed
 * - {object} [delOptions]: options for metadata to delete the null
     version key, if needed
 */
function processVersioningState(mst, vstat, nullVersionCompatMode) {
    const versioningSuspended = (vstat === 'Suspended');
    const masterIsNull = mst.exists && (mst.isNull || !mst.versionId);

    if (versioningSuspended) {
        // versioning is suspended: overwrite the existing null version
        const options = { versionId: '', isNull: true };
        if (masterIsNull) {
            // if the null version exists, clean it up prior to put
            if (mst.objLocation) {
                options.dataToDelete = mst.objLocation;
            }
            // backward-compat: a null version key may exist even with
            // a null master (due to S3C-7526), if so, delete it (its
            // data will be deleted as part of the master cleanup, so
            // no "deleteData" param is needed)
            //
            // "isNull2" attribute is set in master metadata when
            // null keys are used, which is used as an optimization to
            // avoid having to check the versioned key since there can
            // be no more versioned key to clean up
            if (mst.isNull && !mst.isNull2) {
                const delOptions = { versionId: mst.versionId };
                return { options, delOptions };
            }
            return { options };
        }
        if (mst.nullVersionId) {
            // backward-compat: delete the null versioned key and data
            const delOptions = { versionId: mst.nullVersionId, deleteData: true };
            if (mst.nullUploadId) {
                delOptions.replayId = mst.nullUploadId;
            }
            return { options, delOptions };
        }
        // clean up the eventual null key's location data prior to put

        // NOTE: due to metadata v1 internal format, we cannot guess
        // from the master key whether there is an associated null
        // key, because the master key may be removed whenever the
        // latest version becomes a delete marker. Hence we need to
        // pessimistically try to get the null key metadata and delete
        // it if it exists.
        const delOptions = { versionId: 'null', deleteData: true };
        return { options, delOptions };
    }

    // versioning is enabled: create a new version
    const options = { versioning: true };
    if (masterIsNull) {
        // if master is a null version or a non-versioned key,
        // copy it to a new null key
        const nullVersionId = mst.isNull ? mst.versionId : nonVersionedObjId;
        if (nullVersionCompatMode) {
            options.extraMD = {
                nullVersionId,
            };
            if (mst.uploadId) {
                options.extraMD.nullUploadId = mst.uploadId;
            }
            return { options, nullVersionId };
        }
        if (mst.isNull && !mst.isNull2) {
            // if master null version was put with an older
            // Cloudserver (or in compat mode), there is a
            // possibility that it also has a null versioned key
            // associated, so we need to delete it as we write the
            // null key
            const delOptions = {
                versionId: nullVersionId,
            };
            return { options, nullVersionId, delOptions };
        }
        return { options, nullVersionId };
    }
    // backward-compat: keep a reference to the existing null
    // versioned key
    if (mst.nullVersionId) {
        options.extraMD = {
            nullVersionId: mst.nullVersionId,
        };
        if (mst.nullUploadId) {
            options.extraMD.nullUploadId = mst.nullUploadId;
        }
    }
    return { options };
}

/**
 * Build the state of the master version from its object metadata
 *
 * @param {object} objMD - object metadata parsed from JSON
 *
 * @return {object} state of master version, with the following attributes:
 * - {boolean} exists - true if the object exists (i.e. if `objMD` is truish)
 * - {string} versionId - version ID of the master key
 * - {boolean} isNull - whether the master version is a null version
 * - {string} nullVersionId - if not a null version, reference to the
 *   null version ID
 * - {array} objLocation - array of data locations
 */
function getMasterState(objMD) {
    if (!objMD) {
        return {};
    }
    const mst = {
        exists: true,
        versionId: objMD.versionId,
        uploadId: objMD.uploadId,
        isNull: objMD.isNull,
        isNull2: objMD.isNull2,
        nullVersionId: objMD.nullVersionId,
        nullUploadId: objMD.nullUploadId,
    };
    if (objMD.location) {
        mst.objLocation = Array.isArray(objMD.location) ?
            objMD.location : [objMD.location];
    }
    return mst;
}
/** versioningPreprocessing - return versioning information for S3 to handle
 * creation of new versions and manage deletion of old data and metadata
 * @param {string} bucketName - name of bucket
 * @param {object} bucketMD - bucket metadata
 * @param {string} objectKey - name of object
 * @param {object} objMD - obj metadata
 * @param {RequestLogger} log - logger instance
 * @param {function} callback - callback
 * @return {undefined} and call callback with params (err, options):
 * options.dataToDelete - (array/undefined) location of data to delete
 * options.versionId - specific versionId to overwrite in metadata
 *  ('' overwrites the master version)
 * options.versioning - (true/undefined) metadata instruction to create new ver
 * options.isNull - (true/undefined) whether new version is null or not
 * options.nullVersionId - if storing a null version in version history, the
 *  version id of the null version
 * options.deleteNullVersionData - whether to delete the data of the null ver
 */
function versioningPreprocessing(bucketName, bucketMD, objectKey, objMD,
    log, callback) {
    const mst = getMasterState(objMD);
    const vCfg = bucketMD.getVersioningConfiguration();
    // bucket is not versioning configured
    if (!vCfg) {
        const options = { dataToDelete: mst.objLocation };
        return process.nextTick(callback, null, options);
    }
    // bucket is versioning configured

    // XXX CLDSRV-355: pass config.nullVersionCompatMode as last param
    // to processVersioningState() when all operations support null keys
    const { options, nullVersionId, delOptions } =
          processVersioningState(mst, vCfg.Status, true);
    return async.series([
        function storeVersion(next) {
            if (!nullVersionId) {
                return process.nextTick(next);
            }
            const versionMD = Object.assign({}, objMD, { versionId: nullVersionId, isNull: true });
            const params = { versionId: nullVersionId };
            return _storeNullVersionMD(bucketName, objectKey, versionMD, params, log, next);
        },
        function deleteNullVersion(next) {
            if (!delOptions) {
                return process.nextTick(next);
            }
            return _deleteNullVersionMD(bucketName, objectKey, delOptions, mst,
                log, (err, nullDataToDelete) => {
                    if (err) {
                        log.warn('unexpected error deleting null version md', {
                            error: err,
                            method: 'versioningPreprocessing',
                        });
                        // it's possible there was a concurrent request to
                        // delete the null version, so proceed with putting a
                        // new version
                        if (err.is.NoSuchKey) {
                            return next(null, options);
                        }
                        return next(errors.InternalError);
                    }
                    Object.assign(options, { dataToDelete: nullDataToDelete });
                    return next();
                });
        },
    ], err => callback(err, options));
}

/** preprocessingVersioningDelete - return versioning information for S3 to
 * manage deletion of objects and versions, including creation of delete markers
 * @param {string} bucketName - name of bucket
 * @param {object} bucketMD - bucket metadata
 * @param {object} objectMD - obj metadata
 * @param {string} [reqVersionId] - specific version ID sent as part of request
 * @return {object} options object with params:
 * options.deleteData - (true/undefined) whether to delete data (if undefined
 *  means creating a delete marker instead)
 * options.versionId - specific versionId to delete
 */
function preprocessingVersioningDelete(bucketName, bucketMD, objectMD, reqVersionId) {
    const options = {};
    if (!bucketMD.getVersioningConfiguration() || reqVersionId) {
        // delete data if bucket is non-versioned or the request
        // deletes a specific version
        options.deleteData = true;
    }
    if (bucketMD.getVersioningConfiguration() && reqVersionId) {
        if (reqVersionId === 'null') {
            // deleting the 'null' version if it exists: use its
            // internal versionId if it exists
            if (objectMD.versionId !== undefined) {
                options.versionId = objectMD.versionId;
            }
        } else {
            // deleting a specific version
            options.versionId = reqVersionId;
        }
    }
    return options;
}

module.exports = {
    decodeVersionId,
    getVersionIdResHeader,
    checkQueryVersionId,
    processVersioningState,
    getMasterState,
    versioningPreprocessing,
    preprocessingVersioningDelete,
};
