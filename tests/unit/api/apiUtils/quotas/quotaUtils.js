const sinon = require('sinon');
const assert = require('assert');
const { config } = require('../../../../../lib/Config');
const {
    validateQuotas,
    processBytesToWrite,
    isMetricStale,
} = require('../../../../../lib/api/apiUtils/quotas/quotaUtils');
const QuotaService = require('../../../../../lib/quotas/quotas');

const mockLog = {
    warn: sinon.stub(),
    debug: sinon.stub(),
};

const mockBucket = {
    getQuota: () => 100,
    getName: () => 'bucketName',
    getCreationDate: () => '2022-01-01T00:00:00.000Z',
};

const mockBucketNoQuota = {
    getQuota: () => 0,
    getName: () => 'bucketName',
    getCreationDate: () => '2022-01-01T00:00:00.000Z',
};

describe('validateQuotas (buckets)', () => {
    const request = {
        getQuota: () => 100,
    };

    beforeEach(() => {
        config.scuba = {
            host: 'localhost',
            port: 8080,
        };
        config.quota = {
            maxStaleness: 24 * 60 * 60 * 1000,
            enableInflights: true,
        };
        config.isQuotaEnabled = sinon.stub().returns(true);
        QuotaService.enabled = true;
        QuotaService._getLatestMetricsCallback = sinon.stub().resolves({});
        request.finalizerHooks = [];
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should return null if quota is <= 0', done => {
        validateQuotas(request, mockBucketNoQuota, {}, [], '', false, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.called, false);
            done();
        });
    });

    it('should return null if scuba is disabled', done => {
        QuotaService.enabled = false;
        validateQuotas(request, mockBucket, {}, [], '', false, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.called, false);
            done();
        });
    });

    it('should return null if metrics retrieval fails', done => {
        QuotaService.enabled = true;
        const error = new Error('Failed to get metrics');
        QuotaService._getLatestMetricsCallback.yields(error);

        validateQuotas(request, mockBucket, {}, ['objectPut', 'getObject'], 'objectPut', 1, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledOnce, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectPut',
                    inflight: 1,
                }
            ), true);
            done();
        });
    });

    it('should return errors.QuotaExceeded if quota is exceeded', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectPut', 'getObject'], 'objectPut', 1, false, mockLog, err => {
            assert.strictEqual(err.is.QuotaExceeded, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 1);
            assert.strictEqual(request.finalizerHooks.length, 1);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectPut',
                    inflight: 1,
                }
            ), true);
            done();
        });
    });

    it('should not return QuotaExceeded if quotas are exceeded but operation is creating a delete marker', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectDelete'], 'objectDelete', 0, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledOnce, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectDelete',
                    inflight: 0,
                }
            ), true);
            done();
        });
    });

    it('should not return QuotaExceeded if the quotas are exceeded but operation is a delete', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectDelete'], 'objectDelete', -50, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledOnce, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectDelete',
                    inflight: -50,
                }
            ), true);
            done();
        });
    });

    it('should not return QuotaExceeded if the quotas are exceeded but operation is a delete with version', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectDelete'], 'objectDeleteVersion', -50, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledOnce, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectDelete',
                    inflight: -50,
                }
            ), true);
            done();
        });
    });

    it('should decrease the inflights by deleting data, and go below 0 to unblock operations', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectDelete'], 'objectDelete', -5000, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledOnce, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectDelete',
                    inflight: -5000,
                }
            ), true);
            done();
        });
    });

    it('should return null if quota is not exceeded', done => {
        const result1 = {
            bytesTotal: 80,
        };
        const result2 = {
            bytesTotal: 90,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectRestore', 'objectPut'], 'objectRestore',
            true, false, mockLog, err => {
                assert.ifError(err);
                assert.strictEqual(QuotaService._getLatestMetricsCallback.calledTwice, true);
                assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                    'bucket',
                    'bucketName_1640995200000',
                    null,
                    {
                        action: 'objectRestore',
                        inflight: true,
                    }
                ), true);
                done();
            });
    });

    it('should not include the inflights in the request if they are disabled', done => {
        config.quota.enableInflights = false;
        const result1 = {
            bytesTotal: 80,
        };
        const result2 = {
            bytesTotal: 90,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectRestore', 'objectPut'], 'objectRestore',
            true, false, mockLog, err => {
                assert.ifError(err);
                assert.strictEqual(QuotaService._getLatestMetricsCallback.calledTwice, true);
                assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                    'bucket',
                    'bucketName_1640995200000',
                    null,
                    {
                        action: 'objectRestore',
                        inflight: undefined,
                    }
                ), true);
            done();
        });
    });

    it('should evaluate the quotas and not update the inflights when isStorageReserved is true', done => {
        const result1 = {
            bytesTotal: 80,
        };
        const result2 = {
            bytesTotal: 90,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {}, ['objectPut'], 'objectPut',
            true, true, mockLog, err => {
                assert.ifError(err);
                assert.strictEqual(QuotaService._getLatestMetricsCallback.calledOnce, true);
                assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                    'bucket',
                    'bucketName_1640995200000',
                    null,
                    {
                        action: 'objectPut',
                        inflight: 0,
                    }
                ), true);
                done();
            });
    });
});

describe('validateQuotas (with accounts)', () => {
    const request = {
        getQuota: () => 100,
    };

    beforeEach(() => {
        config.scuba = {
            host: 'localhost',
            port: 8080,
        };
        config.quota = {
            maxStaleness: 24 * 60 * 60 * 1000,
            enableInflights: true,
        };
        request.finalizerHooks = [];
        config.isQuotaEnabled = sinon.stub().returns(true);
        QuotaService.enabled = true;
        QuotaService._getLatestMetricsCallback = sinon.stub().resolves({});
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should return null if quota is <= 0', done => {
        validateQuotas(request, mockBucketNoQuota, {
            account: 'test_1',
            quota: 0,
        }, [], '', false, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.called, false);
            done();
        });
    });

    it('should not return null if bucket quota is <= 0 but account quota is > 0', done => {
        validateQuotas(request, mockBucketNoQuota, {
            account: 'test_1',
            quota: 1000,
        }, [], '', false, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.called, false);
            done();
        });
    });

    it('should return null if scuba is disabled', done => {
        QuotaService.enabled = false;
        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, [], '', false, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.called, false);
            done();
        });
    });

    it('should return null if metrics retrieval fails', done => {
        QuotaService.enabled = true;
        const error = new Error('Failed to get metrics');
        QuotaService._getLatestMetricsCallback.yields(error);

        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, ['objectPut', 'getObject'], 'objectPut', 1, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledOnce, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'bucket',
                'bucketName_1640995200000',
                null,
                {
                    action: 'objectPut',
                    inflight: 1,
                }
            ), true);
            done();
        });
    });

    it('should return errors.QuotaExceeded if quota is exceeded', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucketNoQuota, {
            account: 'test_1',
            quota: 100,
        }, ['objectPut', 'getObject'], 'objectPut', 1, false, mockLog, err => {
            assert.strictEqual(err.is.QuotaExceeded, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 1);
            assert.strictEqual(request.finalizerHooks.length, 1);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'account',
                'test_1',
                null,
                {
                    action: 'objectPut',
                    inflight: 1,
                }
            ), true);
            done();
        });
    });

    it('should not return QuotaExceeded if the quotas are exceeded but operation is a delete', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucketNoQuota, {
            account: 'test_1',
            quota: 1000,
        }, ['objectDelete'], 'objectDelete', -50, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 1);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'account',
                'test_1',
                null,
                {
                    action: 'objectDelete',
                    inflight: -50,
                }
            ), true);
            done();
        });
    });

    it('should decrease the inflights by deleting data, and go below 0 to unblock operations', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucketNoQuota, {
            account: 'test_1',
            quota: 1000,
        }, ['objectDelete'], 'objectDelete', -5000, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 1);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'account',
                'test_1',
                null,
                {
                    action: 'objectDelete',
                    inflight: -5000,
                }
            ), true);
            done();
        });
    });

    it('should return null if quota is not exceeded', done => {
        const result1 = {
            bytesTotal: 80,
        };
        const result2 = {
            bytesTotal: 90,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, ['objectRestore', 'objectPut'], 'objectRestore', true, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 4);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'account',
                'test_1',
                null,
                {
                    action: 'objectRestore',
                    inflight: true,
                }
            ), true);
            done();
        });
    });

    it('should return quota exceeded if account and bucket quotas are different', done => {
        const result1 = {
            bytesTotal: 150,
        };
        const result2 = {
            bytesTotal: 120,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, ['objectPut', 'getObject'], 'objectPut', 1, false, mockLog, err => {
            assert.strictEqual(err.is.QuotaExceeded, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 2);
            assert.strictEqual(request.finalizerHooks.length, 1);
            done();
        });
    });

    it('should update the request with one function per action to clear quota updates', done => {
        const result1 = {
            bytesTotal: 80,
        };
        const result2 = {
            bytesTotal: 90,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, ['objectRestore', 'objectPut'], 'objectRestore', true, false, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.callCount, 4);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'account',
                'test_1',
                null,
                {
                    action: 'objectRestore',
                    inflight: true,
                }
            ), true);
            done();
        });
    });

    it('should evaluate the quotas and not update the inflights when isStorageReserved is true', done => {
        const result1 = {
            bytesTotal: 80,
        };
        const result2 = {
            bytesTotal: 90,
        };
        QuotaService._getLatestMetricsCallback.yields(null, result1);
        QuotaService._getLatestMetricsCallback.onCall(1).yields(null, result2);

        validateQuotas(request, mockBucket, {
            account: 'test_1',
            quota: 1000,
        }, ['objectPut'], 'objectPut', true, true, mockLog, err => {
            assert.ifError(err);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledTwice, true);
            assert.strictEqual(QuotaService._getLatestMetricsCallback.calledWith(
                'account',
                'test_1',
                null,
                {
                    action: 'objectPut',
                    inflight: 0,
                }
            ), true);
            done();
        });
    });
});

describe('processBytesToWrite', () => {
    let bucket;
    let versionId;
    let contentLength;
    let objMD;

    beforeEach(() => {
        bucket = {
            isVersioningEnabled: sinon.stub(),
        };
        versionId = '';
        contentLength = 0;
        objMD = null;
    });

    const hotObject = {
        'content-length': 100,
        dataStoreName: 'eu-west-1',
    };
    const coldObject = {
        ...hotObject,
        dataStoreName: 'glacier',
        archive: {
            archiveInfo: '{archiveID,archiveVersion}'
        },
    };
    const restoringObject = {
        ...coldObject,
        archive: {
            ...coldObject.archive,
            restoreRequestedAt: new Date(Date.now() - 2 * 3600 * 1000).toISOString(),
            restoreRequestedDays: 1,
        },
    };
    const restoredObject = {
        ...restoringObject,
        dataStoreName: 'eu-west-1',
        'x-amz-storage-class': 'glacier',
        archive: {
            ...restoringObject.archive,
            restoreCompletedAt: new Date(Date.now() - 3600 * 1000),
            restoreWillExpireAt: new Date(Date.now() + 23 * 3600 * 1000),
        },
    };
    const expiredObject = {
        ...restoredObject,
        archive: {
            ...coldObject.archive,
            restoreRequestedAt: new Date(Date.now() - 25 * 3600 * 1000 - 1000).toString(),
            restoreRequestedDays: 1,
            restoreCompletedAt: new Date(Date.now() - 24 * 3600 * 1000 - 1000),
            restoreWillExpireAt: new Date(Date.now() - 1000),
        },
    };

    [
        // non versionned case
        ['the content-length when deleting hot object', hotObject, false, undefined, -100],
        ['0 when deleting cold object', coldObject, false, undefined, 0],
        ['the content-length when deleting restoring object', restoringObject, false, undefined, -100],
        ['the content-length when deleting restored object', restoredObject, false, undefined, -100],
        ['the content-length when deleting expired object', expiredObject, false, undefined, -100],

        // versionned case
        ['the content-length when deleting hot object version', hotObject, true, 'versionId', -100],
        ['0 when deleting cold versioned object version', coldObject, true, 'versionId', 0],
        ['the content-length when deleting restoring object version', restoringObject, true, 'versionId', -100],
        ['the content-length when deleting restored object version', restoredObject, true, 'versionId', -100],
        ['the content-length when deleting expired object version', expiredObject, true, 'versionId', -100],

        // delete marker case
        ['0 when adding delete marker over hot object', hotObject, true, undefined, 0],
        ['0 when adding delete marker over cold object', coldObject, true, undefined, 0],
        ['0 when adding delete marker over restoring object', restoringObject, true, undefined, 0],
        ['0 when adding delete marker over restored object', restoredObject, true, undefined, 0],
        ['0 when adding delete marker over expired object', expiredObject, true, undefined, 0],
    ].forEach(([scenario, object, versionned, reqVersionId, expected]) => {
        it(`should return ${scenario}`, () => {
            bucket.isVersioningEnabled.returns(versionned);
            objMD = object;

            const bytes = processBytesToWrite('objectDelete', bucket, reqVersionId, 0, objMD);

            assert.strictEqual(bytes, expected);
        });
    });

    it('should return 0 for a delete operation if the object metadata is missing', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = null;

        const bytes = processBytesToWrite('objectDelete', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, 0);
    });

    it('should return the object metadata content length for a restore object operation', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };
        contentLength = 150;

        const bytes = processBytesToWrite('objectRestore', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, 100);
    });

    it('should return the difference of the content length if the object is being replaced', () => {
        bucket.isVersioningEnabled.returns(false);
        objMD = { 'content-length': 100 };
        contentLength = 150;

        const bytes = processBytesToWrite('objectPut', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, 50);
    });

    it('should return content length if the object is being replaced and the bucket is versioned', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };
        contentLength = 150;

        const bytes = processBytesToWrite('objectPut', bucket, versionId, contentLength, objMD);

        assert.strictEqual(bytes, contentLength);
    });

    it('should return content length of the object metadata if the action is a copy (getObject authz)', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };

        const bytes = processBytesToWrite('objectCopy', bucket, versionId, 0, objMD);

        assert.strictEqual(bytes, 100);
    });

    it('should return content length of the object metadata if the action is a copy part (getObject authz)', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };

        const bytes = processBytesToWrite('objectPutCopyPart', bucket, versionId, 0, objMD);

        assert.strictEqual(bytes, 100);
    });

    it('should detect object replacement during copy object operation on a non versioned bucket', () => {
        bucket.isVersioningEnabled.returns(false);
        objMD = { 'content-length': 100 };
        const destObjMD = { 'content-length': 20 };

        const bytes = processBytesToWrite('objectCopy', bucket, versionId, 0, objMD, destObjMD);

        assert.strictEqual(bytes, 80);
    });

    it('should not detect object replacement during copy object operation if the bucket is versioned', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };
        const destObjMD = { 'content-length': 20 };

        const bytes = processBytesToWrite('objectCopy', bucket, versionId, 0, objMD, destObjMD);

        assert.strictEqual(bytes, 100);
    });

    it('should not detect object replacement during copy object operation if the object is cold', () => {
        bucket.isVersioningEnabled.returns(true);
        objMD = { 'content-length': 100 };
        const destObjMD = coldObject;

        const bytes = processBytesToWrite('objectCopy', bucket, versionId, 0, objMD, destObjMD);

        assert.strictEqual(bytes, 100);
    });
});

describe('isMetricStale', () => {
    const metric = {
        date: new Date().toISOString(),
    };
    const resourceType = 'bucket';
    const resourceName = 'bucketName';
    const action = 'objectPut';
    const inflight = 1;
    const log = {
        warn: sinon.stub(),
    };

    it('should return false if the metric is not stale', () => {
        const result = isMetricStale(metric, resourceType, resourceName, action, inflight, log);
        assert.strictEqual(result, false);
        assert.strictEqual(log.warn.called, false);
    });

    it('should return true and log a warning if the metric is stale', () => {
        const staleDate = new Date(Date.now() - 24 * 60 * 60 * 1000 - 1);
        metric.date = staleDate.toISOString();

        const result = isMetricStale(metric, resourceType, resourceName, action, inflight, log);
        assert.strictEqual(result, true);
        assert.strictEqual(log.warn.calledOnce, true);
    });
});
