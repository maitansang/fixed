const util = require('util');
const exec = util.promisify(require('child_process').exec);

exports.allAccess = (req, res) => {
  res.status(200).send("Public Content.");
};

exports.userBoard = (req, res) => {
  res.status(200).send("User Content.");
};

exports.adminBoard = (req, res) => {
  res.status(200).send("Admin Content.");
};

exports.moderatorBoard = (req, res) => {
  res.status(200).send("Moderator Content.");
};

exports.patternFeatures = (req, res) => {
  async function lsExample() {
    try {
      await exec('cd ..');
      const { stdout, stderr } = await exec('cd .. && cd pattern_features && pwd && go run main.go 2021-01-01 2021-01-02');
      console.log('stdout:', stdout);
      console.log('stderr:', stderr);
    } catch (e) {
      console.error(e); // should contain code (exit code) and signal (that caused the termination).
    }
  }
  lsExample()
  res.status(200).send("pattern features.");
};
