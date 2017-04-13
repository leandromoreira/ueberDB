var expect = require('expect.js');
var newMongoDB = require('../new_mongodb_db');

describe('the new mongodb adapter', function() {
  describe('mandatory values on "settings"', function() {
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
});
