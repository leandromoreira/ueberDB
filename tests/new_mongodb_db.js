var expect = require('expect.js');

var newMongoDB          = require('../new_mongodb_db');
var defaultTestSettings = require('../defaultTestSettings.js');
var ueberDB             = require('../CloneAndAtomicLayer');

describe('the new mongodb adapter', function() {
  describe('mandatory values on "settings" of newMongoDB.database(settings)', function() {
    var settings;
    var subject = function() { newMongoDB.database(settings) };

    beforeEach(function() {
      // initiate settings with mandatory values
      settings = {
        host: 'the host',
        dbname: 'the db name',
        port: 1234,
      };
    });

    it('requires settings', function() {
      settings = null;
      expect(subject).to.throwException();
    });

    context('when settings.url is not provided', function() {
      beforeEach(function() {
        delete settings.url;
      });

      it('requires settings.host', function() {
        delete settings.host;
        expect(subject).to.throwException();
      });

      it('requires settings.dbname', function() {
        delete settings.dbname;
        expect(subject).to.throwException();
      });

      it('requires settings.port', function() {
        delete settings.port;
        expect(subject).to.throwException();
      });
    });

    context('when settings.url is provided', function() {
      beforeEach(function() {
        settings.url = 'the url';
      });

      it('does not require settings.host', function() {
        delete settings.host;
        expect(subject).to.not.throwException();
      });

      it('does not require settings.dbname', function() {
        delete settings.dbname;
        expect(subject).to.not.throwException();
      });

      it('does not require settings.port', function() {
        delete settings.port;
        expect(subject).to.not.throwException();
      });
    });
  });

  describe('.set() and .get()', function() {
    var KEY = 'the key';

    var db;

    before(function(done) {
      db = new ueberDB.database('new_mongodb', defaultTestSettings['new_mongodb']);
      db.init(done);
    });

    after(function(done) {
      db.close(done);
    });

    it('creates a record and retrieves it', function(done) {
      var value = 'the value';

      db.set(KEY, value, null, function() {
        db.get(KEY, function(err, valueFound) {
          expect(valueFound).to.be(value);
          done();
        });
      });
    });

    it('returns null when the original record value is null', function(done) {
      var value = null;

      db.set(KEY, value, null, function() {
        db.get(KEY, function(err, valueFound) {
          expect(valueFound).to.be(null);
          done();
        });
      });
    });

  });

  describe('.findKeys()', function() {
    var db;

    before(function(done) {
      db = new ueberDB.database('new_mongodb', defaultTestSettings['new_mongodb']);
      db.init(function() {
        // set initial values as on the example of
        // https://github.com/Pita/ueberDB/wiki/findKeys-functionality#how-it-works
        db.set('test:id1', 'VALUE', null, function() {
          db.set('test:id1:chat:id2', 'VALUE', null, function() {
            db.set('chat:id3:test:id4', 'VALUE', null, done);
          });
        });
      });
    });

    after(function(done) {
      db.close(done);
    });

    it('returns all matched keys when "notkey" is null', function(done) {
      db.findKeys('test:*', null, function(err, keysFound) {
        expect(keysFound).to.have.length(2);
        expect(keysFound).to.contain('test:id1');
        expect(keysFound).to.contain('test:id1:chat:id2');
        done();
      });
    });

    // same scenario of https://github.com/Pita/ueberDB/wiki/findKeys-functionality
    it('returns the only matched "key" that does not match "notkey"', function(done) {
      db.findKeys('test:*', '*:*:*', function(err, keysFound) {
        expect(keysFound).to.have.length(1);
        expect(keysFound).to.contain('test:id1');
        done();
      });
    });

    it('returns an empty array when no key is found', function(done) {
      db.findKeys('nomatch', null, function(err, keysFound) {
        expect(keysFound).to.have.length(0);
        done();
      });
    });
  });
});
