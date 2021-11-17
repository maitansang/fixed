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
  const ticker = req.body.ticker
  const startDate = req.body.startDate
  const endDate = req.body.endDate
  async function lsExample() {
    try {
      await exec('cd ..');
      const { stdout, stderr } = await exec(`cd .. && cd pattern_features && pwd && go run main.go ${ticker} ${endDate} ${startDate}`);
      console.log('stdout:', stdout);
      console.log('stderr:', stderr);
      res.status(200).send("success");

    } catch (e) {
      console.error(e); // should contain code (exit code) and signal (that caused the termination).
      res.status(500).send("fail");
    }
  }
  lsExample()
};
