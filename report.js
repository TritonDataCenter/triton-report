#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

var assert = require('assert');
var async = require('async');
var fs = require('fs');
var manta = require('manta');
var path = require('path');
var sprintf = require('sprintf-js').sprintf;
var tabula = require('tabula');
var zlib = require('zlib');

// GLOBAL
var ADMIN_UUID = process.env.ADMIN_UUID;
var DATACENTER = process.env.DATACENTER;
var DOCKER_IMAGES = {};
var IMGAPI_IMAGES = {};
var client = manta.createClient({
    sign: null,
    user: process.env.MANTA_USER,
    url: process.env.MANTA_URL
});

// general helpers

function dockerIdToUuid(dockerId) {
    var out;

    out = dockerId.substr(0, 8) + '-'
        + dockerId.substr(8, 4) + '-'
        + dockerId.substr(12, 4) + '-'
        + dockerId.substr(16, 4) + '-'
        + dockerId.substr(20, 12);

    return (out);
}

function getFirstDatafile(dir, fileprefix, callback) {
    var opts = {
        offset: 0,
        limit: 256,
        type: 'object'
    };
    var result;

    client.ls(dir, opts, function (err, res) {
        if (err && err.code === 'NotFoundError') {
            callback(null, result);
            return;
        } else if (err) {
            callback(err);
            return;
        }

        res.on('object', function (obj) {
            if (!result) {
                if (fileprefix) {
                    if (obj.name.substr(0, fileprefix.length) === fileprefix) {
                        result = obj;
                    }
                } else {
                    result = obj;
                }
            }
        });

        res.once('error', function (e) {
            console.error('ERROR: ' + e.stack);
            process.exit(1);
        });

        res.once('end', function () {
            callback(null, result);
        });
    });
}

// data finders

function findCnapiData(datacenter, callback)
{
    findLatestDatafile(datacenter, 'cnapi_servers', null, callback);
}

function findImgapiData(datacenter, callback)
{
    findLatestDatafile(datacenter, 'imgapi_images', null, callback);
}

function findVmapiData(datacenter, callback)
{
    findLatestDatafile(datacenter, 'manatee_backups', 'vmapi_vms', callback);
}

function findDockerImageData(datacenter, callback)
{
    findLatestDatafile(datacenter, 'manatee_backups', 'docker_image_tags',
        callback);
}

function findUserData(datacenter, callback)
{
    findLatestDatafile(datacenter, 'manatee_backups', 'ufds_o_smartdc',
        callback);
}

// general log processor

function onEachLogLine(datacenter, service, hours, objcb, callback)
{
    var idx = 0;
    var path_prefix = path.join('/admin/stor/logs', datacenter, service);

    async.whilst(function () {
        return (idx < hours);
    }, function (cb) {
        var when;
        var year, month, day, hour;
        var potential;

        when = new Date(new Date().getTime() - (1000 * 3600 * (hours - idx)));

        year = when.getUTCFullYear().toString();
        month = (when.getUTCMonth() + 1).toString();
        day = when.getUTCDate().toString();
        hour = when.getUTCHours().toString();
        month = (month.length === 1 ? '0' + month : month);
        day = (day.length === 1 ? '0' + day : day);
        hour = (hour.length === 1 ? '0' + hour : hour);

        potential = path.join(path_prefix, year, month, day, hour);

        getFirstDatafile(potential, null, function (err, value) {
            var buffer = '';
            var filename;

            if (!err && value) {
                filename = path.join(value.parent, value.name);
                console.error('=> ' + filename);

                readFile(filename, function (line) {
                    var chunk;
                    var chunks;

                    buffer += line.toString();
                    chunks = buffer.split('\n');
                    while (chunks.length > 1) {
                        chunk = chunks.shift();
                        if (chunk && chunk.length > 0) {
                            try {
                                objcb(JSON.parse(chunk));
                            } catch (e) {
                                // TODO: if this line looks like
                                //     "Uncaught TypeError: Cannot call method
                                //     'substr' of undefined"
                                // then we should track that separately.
                                console.error('failed to parse: '
                                    + JSON.stringify(chunk));
                            }
                        }
                    }
                    buffer = chunks.pop();
                    return;
                }, function (e) {
                    if (!e && buffer.length > 0) {
                        objcb(JSON.parse(buffer));
                    }
                    cb(e);
                    idx++;
                });
            } else {
                idx++;
                cb();
            }
        });
    }, function (err) {
        callback();
    });
}

function findLatestDatafile(datacenter, key, fileprefix, callback) {
    var found_file = '';
    var hours_ago = 0;
    var path_prefix = path.join('/admin/stor/sdc', key, datacenter);
    var year, month, day, hour;

    async.whilst(function () {
        return (found_file.length === 0 && hours_ago <= 24);
    }, function (cb) {
        var potential;
        var when;

        when = new Date(new Date().getTime() - (1000 * 3600 * hours_ago));
        year = when.getUTCFullYear().toString();
        month = (when.getUTCMonth() + 1).toString();
        day = when.getUTCDate().toString();
        hour = when.getUTCHours().toString();
        month = (month.length === 1 ? '0' + month : month);
        day = (day.length === 1 ? '0' + day : day);
        hour = (hour.length === 1 ? '0' + hour : hour);

        potential = path.join(path_prefix, year, month, day, hour);

        getFirstDatafile(potential, fileprefix, function (err, value) {
            if (value) {
                found_file = value;
            }
            hours_ago++;
            cb();
        });
    }, function (err) {
        console.error('=> ' + path.join(found_file.parent, found_file.name));
        callback(null, path.join(found_file.parent, found_file.name));
    });
}

function findFiles(callback) {
    var files = {};

    async.series([
        function (cb) {
            findCnapiData(DATACENTER, function (err, filename) {
                files.cnapi = filename;
                cb(err);
            });
        }, function (cb) {
            findVmapiData(DATACENTER, function (err, filename) {
                files.vmapi = filename;
                cb(err);
            });
        }, function (cb) {
            findDockerImageData(DATACENTER, function (err, filename) {
                files.dockerimages = filename;
                cb(err);
            });
        }, function (cb) {
            findImgapiData(DATACENTER, function (err, filename) {
                files.imgapi = filename;
                cb(err);
            });
        }, function (cb) {
            findUserData(DATACENTER, function (err, filename) {
                files.ufds = filename;
                cb(err);
            });
        }
    ], function (err) {
        callback(err, files);
    });
}

function readFile(filename, linecb, callback) {
    var gunzip = zlib.createGunzip();

    client.get(filename, function (err, get_stream) {
        var stream;

        if (err) {
            callback(err);
            return;
        }

        if (filename.match(/.gz$/)) {
            get_stream.pipe(gunzip);
            stream = gunzip;
        } else {
            stream = get_stream;
        }

        stream.setEncoding('utf8');
        stream.on('data', function (chunk) {
            linecb(chunk);
        });
        stream.on('end', function () {
            callback();
        });
    });
}

// CNAPI servers

function processCnapiServer(server, data) {
    var pct;

    if (server.hostname === 'headnode') {
        return;
    }

    pct = server.unreserved_ram
        / (server.ram - (server.ram * server.reservation_ratio)) * 100;

    if (!data.servers) {
        data.servers = [];
    }

    data.servers.push({
        hostname: server.hostname,
        ram: server.ram,
        prov: sprintf('%5.2f%%', pct),
        vms: Object.keys(server.vms).length,
        traits: ((server.traits && Object.keys(server.traits).length > 0) ?
            ' ' + JSON.stringify(Object.keys(server.traits)) : '')
    });
}

function processCnapiData(filename, data, callback) {
    var buffer = '';

    readFile(filename, function (line) {
        buffer += line.toString();
    }, function (err) {
        var obj;
        var idx;

        if (!err) {
            idx = buffer.indexOf('[\n') || 0;
            obj = JSON.parse(buffer.slice(idx));
            obj.forEach(function (server) {
                processCnapiServer(server, data);
            });
        }
        callback(err);
    });
}

// IMGAPI images

function processImgapiImg(imgobj, data) {
    if (!data.hasOwnProperty('images')) {
        data.images = {};
    }
    data.images[imgobj.uuid] = imgobj.name + ':' + imgobj.version;
}

function processImgapiData(filename, data, callback) {
    var buffer = '';

    readFile(filename, function (line) {
        buffer += line.toString();
    }, function (err) {
        var idx;
        var obj;

        if (!err) {
            idx = buffer.indexOf('[\n') || 0;
            obj = JSON.parse(buffer.slice(idx));
            obj.forEach(function (img) {
                processImgapiImg(img, data);
            });
        }
        callback(err);
    });
}

// Docker images

function processDockerImg(imgobj, data) {
    if (!data.hasOwnProperty('dockerimages')) {
        data.dockerimages = {};
    }
    data.dockerimages[dockerIdToUuid(imgobj.docker_id)]
        = imgobj.repo + ':' + imgobj.tag;
}

function processDockerImgData(filename, data, callback) {
    var buffer = '';

    function addData(chunk) {
        var imgobj;
        var obj;

        if (chunk.length === 0) {
            return;
        }

        try {
            obj = JSON.parse(chunk);
        } catch (e) {
            console.error('FAILED TO PARSE: ' + chunk);
        }

        if (obj.entry) {
            imgobj = JSON.parse(obj.entry[3]);
            processDockerImg(imgobj, data);
        }
    }

    readFile(filename, function (_data) {
        var chunk;
        var chunks;

        buffer += _data.toString();
        chunks = buffer.split('\n');
        while (chunks.length > 1) {
            chunk = chunks.shift();
            addData(chunk);
        }
        buffer = chunks.pop();
        return;
    }, function (err) {
        addData(buffer);
        callback(err);
    });
}

// VMAPI VMs

function updateVMCounters(vmobj, data) {
    var bucket;
    var life;
    var now = (new Date()).getTime;
    var policy;
    var type;

    // images

    if (vmobj.image_uuid) {
        if (!data.hasOwnProperty('vm_images_all')) {
            data.vm_images_all = {};
        }
        if (!data.vm_images_all.hasOwnProperty(vmobj.image_uuid)) {
            data.vm_images_all[vmobj.image_uuid] = 0;
        }
        data.vm_images_all[vmobj.image_uuid]++;
        if (!vmobj.destroyed) {
            if (!data.hasOwnProperty('vm_images_active')) {
                data.vm_images_active = {};
            }
            if (!data.vm_images_active.hasOwnProperty(vmobj.image_uuid)) {
                data.vm_images_active[vmobj.image_uuid] = 0;
            }
            data.vm_images_active[vmobj.image_uuid]++;
        }
    }

    // lifetimes

    if (!vmobj.create_timestamp) {
        life = 0;
    } else if (vmobj.destroyed) {
        life = vmobj.destroyed - vmobj.create_timestamp;
    } else {
        life = now - vmobj.create_timestamp;
    }
    bucket = getLifetimeBucket(life);
    if (!data.hasOwnProperty('vm_lifetimes')) {
        data.vm_lifetimes = {};
        data.vm_life_bucket_idx = {};
        data.vm_lifetime_max = 0;
    }
    if (!data.vm_lifetimes.hasOwnProperty(bucket.name)) {
        data.vm_lifetimes[bucket.name] = 0;
        data.vm_life_bucket_idx[bucket.name] = bucket.idx;
    }
    data.vm_lifetimes[bucket.name]++;
    if (data.vm_lifetimes[bucket.name] > data.vm_lifetime_max) {
        data.vm_lifetime_max = data.vm_lifetimes[bucket.name];
    }

    // restart policies (docker)
    if (vmobj.docker) {
        policy = 'no';
        if (vmobj.restart_policy) {
            policy = vmobj.restart_policy;
        }
        if (!data.hasOwnProperty('vm_restart')) {
            data.vm_restart = {};
        }
        if (!data.vm_restart.hasOwnProperty(policy)) {
            data.vm_restart[policy] = {all: 0};
        }
        if (!data.vm_restart[policy].hasOwnProperty(vmobj.state)) {
            data.vm_restart[policy][vmobj.state] = 0;
        }
        data.vm_restart[policy].all++;
        data.vm_restart[policy][vmobj.state]++;
    }

    // types

    type = vmobj.brand;
    if (vmobj.docker) {
        type = type + '+docker';
    }
    if (vmobj.smartdc_role === 'nat') {
        type = 'nat';
    }
    if (!data.hasOwnProperty('vm_types')) {
        data.vm_types = {};
    }
    if (!data.vm_types.hasOwnProperty(type)) {
        data.vm_types[type] = {all: 0};
    }
    if (!data.vm_types[type].hasOwnProperty(vmobj.state)) {
        data.vm_types[type][vmobj.state] = 0;
    }
    data.vm_types[type].all++;
    data.vm_types[type][vmobj.state]++;

    // states

    if (!data.hasOwnProperty('vm_states')) {
        data.vm_states = {all: {}};
    }
    if (vmobj.create_timestamp) {
        bucket = getStateBucket(vmobj.create_timestamp);
        if (!data.vm_states.hasOwnProperty(bucket.name)) {
            data.vm_states[bucket.name] = {};
        }
        if (!data.vm_states[bucket.name].hasOwnProperty(vmobj.state)) {
            data.vm_states[bucket.name][vmobj.state] = 0;
        }
        data.vm_states[bucket.name][vmobj.state]++;
    }
    if (!data.vm_states.all.hasOwnProperty(vmobj.state)) {
        data.vm_states.all[vmobj.state] = 0;
    }
    data.vm_states.all[vmobj.state]++;

    // sizes

    if (!vmobj.ram) {
        vmobj.ram = 0;
    }
    if (!data.hasOwnProperty('vm_sizes')) {
        data.vm_sizes = {all: {}, active: {}};
        data.vm_size_max_all = 0;
        data.vm_size_max_active = 0;
    }
    if (!data.vm_sizes.all.hasOwnProperty(vmobj.ram)) {
        data.vm_sizes.all[vmobj.ram] = 0;
    }
    data.vm_sizes.all[vmobj.ram]++;
    if (data.vm_sizes.all[vmobj.ram] > data.vm_size_max_all) {
        data.vm_size_max_all = data.vm_sizes.all[vmobj.ram];
    }
    if (!vmobj.destroyed) {
        if (!data.vm_sizes.active.hasOwnProperty(vmobj.ram)) {
            data.vm_sizes.active[vmobj.ram] = 0;
        }
        data.vm_sizes.active[vmobj.ram]++;
        if (data.vm_sizes.active[vmobj.ram] > data.vm_size_max_active) {
            data.vm_size_max_active = data.vm_sizes.active[vmobj.ram];
        }
    }
}

function processVmapiVM(vmobj, data) {
    var im = {};
    var matches;
    var smartdc_role;

    if (vmobj.internal_metadata) {
        im = JSON.parse(vmobj.internal_metadata);
    }

    if (vmobj.tags && vmobj.owner_uuid === ADMIN_UUID) {
        // JSSTYLED
        matches = vmobj.tags.match(/-smartdc_role=([a-z]+)-smartdc_type=core-/);
        if (matches) {
            smartdc_role = matches[1];
        }
    }

    if (!data.hasOwnProperty('vms')) {
        data.vms = {};
    }

    data.vms[vmobj.uuid] = {
        uuid: vmobj.uuid,
        brand: vmobj.brand,
        owner_uuid: vmobj.owner_uuid,
        ram: vmobj.max_physical_memory,
        docker: vmobj.docker,
        restart_policy: im['docker:restartpolicy'],
        state: vmobj.state,
        image_uuid: vmobj.image_uuid,
        create_timestamp: vmobj.create_timestamp,
        destroyed: vmobj.destroyed,
        smartdc_role: smartdc_role
    };

    updateVMCounters(data.vms[vmobj.uuid], data);
}

function processVmapiData(filename, data, callback) {
    var buffer = '';

    function addData(chunk) {
        var obj;
        var vmobj;

        if (chunk.length === 0) {
            return;
        }

        try {
            obj = JSON.parse(chunk);
        } catch (e) {
            console.error('FAILED TO PARSE: ' + chunk);
        }

        if (obj.entry) {
            vmobj = JSON.parse(obj.entry[3]);

            processVmapiVM(vmobj, data);
        }
    }

    readFile(filename, function (_data) {
        var chunk;
        var chunks;

        buffer += _data.toString();
        chunks = buffer.split('\n');
        while (chunks.length > 1) {
            chunk = chunks.shift();
            addData(chunk);
        }
        buffer = chunks.pop();
        return;
    }, function (err) {
        addData(buffer);
        callback(err);
        return;
    });
}

// docker log data

function normalizeDockerEndpoint(endpoint)
{
    var candidate;
    var test;

    // JSSTYLED
    candidate = endpoint.replace(/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/g, '<UUID>');
    candidate = candidate.replace(/[a-f0-9]{64}/g, '<DockerID>');

    test = candidate.match(/^(DELETE \/v[0-9\.]+\/containers\/)/);
    if (test) {
        return (test[1] + '<Container>');
    }
    // JSSTYLED
    test = candidate.match(/^(POST \/v[0-9\.]+\/images\/create\?fromImage=)/);
    if (test) {
        return (test[1] + '<ImageName>');
    }
    // JSSTYLED
    test = candidate.match(/^(POST \/v[0-9\.]+\/containers\/create\?name=)/);
    if (test) {
        return (test[1] + '<Container>');
    }
    test = candidate.match(/^(POST \/v[0-9\.]+\/).*(\/exec)/);
    if (test) {
        return (test[1] + '<Container>' + test[2]);
    }
    test = candidate.match(/^(GET \/v[0-9\.]+\/).*(\/json)/);
    if (test) {
        return (test[1] + '<Container>' + test[2]);
    }
    test = candidate.match(/^(POST \/v[0-9\.]+\/containers\/).*(\/kill.*)$/);
    if (test) {
        return (test[1] + '<Container>' + test[2]);
    }
    test = candidate.match(/^(POST \/v[0-9\.]+\/containers\/).*(\/stop.*)$/);
    if (test) {
        return (test[1] + '<Container>' + test[2]);
    }
    test = candidate.match(/^(POST \/v[0-9\.]+\/containers\/).*(\/start.*)$/);
    if (test) {
        return (test[1] + '<Container>' + test[2]);
    }
    // JSSTYLED
    test = candidate.match(/^(POST \/v[0-9\.]+\/containers\/<DockerID>\/resize\?)/);
    if (test) {
        return (test[1] + 'h=H&w=W');
    }
    test = candidate.match(/^(GET \/v[0-9\.]+\/containers\/).*\/top$/);
    if (test) {
        return (test[1] + '<Container>/top');
    }
    test = candidate.match(/^(GET \/v[0-9\.]+\/containers\/).*\/stats$/);
    if (test) {
        return (test[1] + '<Container>/stats');
    }
    test = candidate.match(/^(POST \/v[0-9\.]+\/containers\/).*(\/attach.*)$/);
    if (test) {
        return (test[1] + '<Container>' + test[2]);
    }
    test = candidate.match(/^(GET \/v[0-9\.]+\/containers\/).*(\/logs.*)$/);
    if (test) {
        return (test[1] + '<Container>' + test[2]);
    }
    // JSSTYLED
    test = candidate.match(/^(GET \/v[0-9\.]+\/images\/search?term=)/);
    if (test) {
        return (test[1] + '<SearchTerm>');
    }
    test = candidate.match(/^(DELETE \/v[0-9\.]+\/images\/)/);
    if (test) {
        return (test[1] + '<ImageName>');
    }
    test = candidate.match(/^(POST \/v[0-9\.]+\/exec\/).*(\/resize?)/);
    if (test) {
        return (test[1] + '<ExecId>' + test[2] + '?h=H&w=W');
    }
    test = candidate.match(/^(POST \/v[0-9\.]+\/containers\/).*(\/resize?)/);
    if (test) {
        return (test[1] + '<Container>' + test[2] + '?h=H&w=W');
    }

    return (candidate);
}

function addDockerLogData(obj, data)
{
    var endpoint;

    if (obj.hasOwnProperty('req') && obj.req.hasOwnProperty('method')) {
        if (!data.hasOwnProperty('methods')) {
            data.methods = {};
        }
        if (!data.methods[obj.req.method]) {
            data.methods[obj.req.method] = 0;
        }
        data.methods[obj.req.method]++;

        if (obj.req.hasOwnProperty('url')) {
            endpoint = normalizeDockerEndpoint(obj.req.method + ' '
                + obj.req.url);
            if (!data.hasOwnProperty('endpoints')) {
                data.endpoints = {};
            }
            if (!data.endpoints[endpoint]) {
                data.endpoints[endpoint] = 0;
            }
            data.endpoints[endpoint]++;
        }
    }

    if (obj.route && obj.res && obj.res.statusCode && obj.latency) {
        if (!data.hasOwnProperty('routes')) {
            data.routes = {};
        }
        if (!data.routes[obj.route]) {
            data.routes[obj.route] = [];
        }
        data.routes[obj.route].push({
            code: obj.res.statusCode,
            latency: obj.latency
        });
    }
}

function processDockerLogData(hours, data, callback) {
    onEachLogLine(DATACENTER, 'docker', hours, function (obj) {
        addDockerLogData(obj, data);
    }, callback);
}

// output helpers

function getLifetimeBucket(life)
{
    var bucket = {};

    if (life < (5 * 60 * 1000)) {
        bucket = {name: '< 5m', idx: 0};
    } else if (life < (30 * 60 * 1000)) {
        bucket = {name: '5-30m', idx: 1};
    } else if (life < (60 * 60 * 1000)) {
        bucket = {name: '30-60m', idx: 2};
    } else if (life < (6 * 60 * 60 * 1000)) {
        bucket = {name: '1-6h', idx: 3};
    } else if (life < (12 * 60 * 60 * 1000)) {
        bucket = {name: '6-12h', idx: 4};
    } else if (life < (24 * 60 * 60 * 1000)) {
        bucket = {name: '12-24h', idx: 5};
    } else if (life < (2 * 24 * 60 * 60 * 1000)) {
        bucket = {name: '1-2d', idx: 6};
    } else if (life < (7 * 24 * 60 * 60 * 1000)) {
        bucket = {name: '2-7d', idx: 7};
    } else {
        bucket = {name: '> 7d', idx: 8};
    }

    return (bucket);
}

function getStateBucket(create_timestamp)
{
    var bucket = {};
    var now = (new Date()).getTime();

    if ((create_timestamp + (24 * 60 * 60 * 1000)) > now) {
        bucket = {name: '< 24h', idx: 0};
    } else if ((create_timestamp + (2 * 24 * 60 * 60 * 1000)) > now) {
        bucket = {name: '1-2d', idx: 1};
    } else if ((create_timestamp + (3 * 24 * 60 * 60 * 1000)) > now) {
        bucket = {name: '2-3d', idx: 2};
    } else if ((create_timestamp + (7 * 24 * 60 * 60 * 1000)) > now) {
        bucket = {name: '3-7d', idx: 3};
    } else if ((create_timestamp + (30 * 24 * 60 * 60 * 1000)) > now) {
        bucket = {name: '7-30d', idx: 4};
    } else {
        bucket = {name: '> 30d', idx: 5};
    }

    return (bucket);
}

function histogram(value, max_value, length)
{
    var count;
    var idx;
    var result = '';
    var unit = Math.ceil(max_value / length);

    count = Math.floor(value / unit);

    for (idx = 0; idx < count; idx++) {
        result = result + '#';
    }

    return (result);
}

function getImageName(uuid, data) {
    if (data.dockerimages[uuid]) {
        return (data.dockerimages[uuid]);
    } else if (data.images[uuid]) {
        return (data.images[uuid]);
    } else {
        return ('unknown');
    }
}

// Output functions

function outputServerCapacity(data) {
    console.log('\n=== CN USAGE ===');
    tabula(data.servers);
}

function outputVmCounts(data) {
    var images = [];

    var lifetime_results = [];
    var restart_results = [];
    var size_results_all = [];
    var size_results_active = [];
    var state_results = [];
    var type_results = [];

    Object.keys(data.vm_states.all).forEach(function (state) {
        var s = {state: state};
        Object.keys(data.vm_states).forEach(function (k) {
            s[k] = data.vm_states[k][state];
        });
        state_results.push(s);
    });
    Object.keys(data.vm_types).forEach(function (type) {
        var t = {type: type};

        Object.keys(data.vm_types[type]).forEach(function (k) {
            t[k] = data.vm_types[type][k];
        });
        type_results.push(t);
    });
    Object.keys(data.vm_lifetimes).forEach(function (lifetime) {
        var hist;

        hist = histogram(data.vm_lifetimes[lifetime], data.vm_lifetime_max, 60);
        lifetime_results.push({
            lifetime: lifetime,
            count: data.vm_lifetimes[lifetime],
            histogram: hist,
            idx: data.vm_life_bucket_idx[lifetime]
        });
    });
    Object.keys(data.vm_restart).forEach(function (policy) {
        var p = {policy: policy};

        Object.keys(data.vm_restart[policy]).forEach(function (k) {
            p[k] = data.vm_restart[policy][k];
        });

        restart_results.push(p);
    });
    Object.keys(data.vm_sizes.all).forEach(function (size) {
        var hist = histogram(data.vm_sizes.all[size], data.vm_size_max_all, 60);
        size_results_all.push({
            size: size,
            count: data.vm_sizes.all[size],
            histogram: hist
        });
    });
    Object.keys(data.vm_sizes.active).forEach(function (size) {
        var hist;

        hist = histogram(data.vm_sizes.active[size],
            data.vm_size_max_active, 60);
        size_results_active.push({
            size: size,
            count: data.vm_sizes.active[size],
            histogram: hist
        });
    });

    console.log('\n=== VM STATES (BY CREATION) ===');
    tabula(state_results, {columns: [
        'state', 'all', '< 24h', '1-2d', '2-3d', '3-7d', '7-30d', '> 30d'
    ], sort: ['all']});
    console.log('\n=== VM LIFETIMES (ALL TIME) ===');
    tabula(lifetime_results, {columns: ['lifetime', 'count', 'histogram'],
        sort: ['idx']});
    console.log('\n=== VM TYPES (ALL TIME) ===');
    tabula(type_results, {sort: ['all']});
    console.log('\n=== VM SIZES (ALL TIME) ===');
    tabula(size_results_all, {columns: ['size', 'count', 'histogram'],
        sort: ['size']});
    console.log('\n=== VM SIZES (ACTIVE) ===');
    tabula(size_results_active, {columns: ['size', 'count', 'histogram'],
        sort: ['size']});
    if (restart_results.length > 0) {
        console.log('\n=== DOCKER RESTART POLICIES ===');
        tabula(restart_results, {sort: ['all']});
    }
    console.log('\n=== TOP 20 IMAGES (ALL TIME) ===');
    images = [];
    Object.keys(data.vm_images_all).sort(function (a, b) {
        return (data.vm_images_all[b] - data.vm_images_all[a]);
    }).slice(0, 20).forEach(function (i) {
        images.push({
            uuid: i,
            name: getImageName(i, data),
            count: data.vm_images_all[i]
        });
    });
    tabula(images, {sort: ['-count']});

    console.log('\n=== TOP 20 IMAGES (ACTIVE) ===');
    images = [];
    Object.keys(data.vm_images_active).sort(function (a, b) {
        return (data.vm_images_active[b] - data.vm_images_active[a]);
    }).slice(0, 20).forEach(function (i) {
        images.push({
            uuid: i,
            name: getImageName(i, data),
            count: data.vm_images_active[i]
        });
    });
    tabula(images, {sort: ['-count']});
}

function latencyHistogram(name, data)
{
    var buckets = {};
    var results = [];
    var min;
    var max;
    var max_count = 0;

    console.log('\n=== ' + name + ' ===');

    data.forEach(function (d) {
        var bucket = 0;
        var bucket_value;

        while (d.latency >= Math.pow(2, bucket)) {
            bucket++;
        }
        bucket_value = Math.pow(2, bucket);

        if (min === undefined || bucket_value < min) {
            min = bucket_value;
        }
        if (max === undefined || bucket_value > max) {
            max = bucket_value;
        }

        if (!buckets[bucket_value]) {
            buckets[bucket_value] = 0;
        }
        buckets[bucket_value]++;
        if (buckets[bucket_value] > max_count) {
            max_count = buckets[bucket_value];
        }
    });

    for (var i = (min / 2); i <= (max * 2); i = i * 2) {
        if (i > 0) {
            if (!buckets.hasOwnProperty(i)) {
                buckets[i] = 0;
            }
        }
    }

    Object.keys(buckets).sort(function (a, b) {
        return (a < b);
    }).forEach(function (f) {
        var hist = histogram(buckets[f], max_count, 60);
        results.push({time: f, count: buckets[f], histogram: hist});
    });

    tabula(results, {columns: ['time', 'count', 'histogram'], sort: ['time']});
}

function outputDockerMethods(data)
{
    var endpoints = [];
    var show_routes = {
        containerattach: 'ContainerAttach',
        containercreate: 'ContainerCreate',
        containerdelete: 'ContainerDelete',
        containerlist: 'ContainerList',
        containerstart: 'ContainerStart',
        imagelist: 'ImageList'
    };

    console.log('\n=== SDC-DOCKER ENDPOINTS (last 24H) ===');
    if (data.hasOwnProperty('endpoints')) {
        Object.keys(data.endpoints).forEach(function (endpoint) {
            endpoints.push({
                count: data.endpoints[endpoint],
                endpoint: endpoint
            });
        });
    }
    tabula(endpoints, {columns: ['count', 'endpoint'], sort: ['-count']});

    if (data.hasOwnProperty('routes')) {
        Object.keys(data.routes).forEach(function (route) {
            if (!show_routes[route]) {
                return;
            }
            latencyHistogram(show_routes[route] + ' Times (ms) Last 24H',
                data.routes[route]);
        });
    }
}

// master data processor dispatcher

function processData(files, callback) {
    var data = {};

    async.series([
        function (cb) {
            processCnapiData(files.cnapi, data, cb);
        }, function (cb) {
            processImgapiData(files.imgapi, data, cb);
        }, function (cb) {
            processDockerImgData(files.dockerimages, data, cb);
        }, function (cb) {
            processVmapiData(files.vmapi, data, cb);
        }, function (cb) {
            processDockerLogData(24, data, cb);
        }
    ], function (err) {
        callback(err, data);
    });
}

findFiles(function (err, files) {
    if (err) {
        console.error('fail: ' + err.message);
        process.exit(1);
    }

    processData(files, function (e, _data) {
        outputServerCapacity(_data);
        outputVmCounts(_data);
        outputDockerMethods(_data);
        process.exit(0);
    });
});
