const gradeMapping = [
  { min: 91, max: 100, gpa: 4 },
  { min: 81, max: 90, gpa: 3.6 },
  { min: 71, max: 80, gpa: 3.2 },
  { min: 61, max: 70, gpa: 2.8 },
  { min: 51, max: 60, gpa: 2.4 },
  { min: 41, max: 50, gpa: 2 },
  { min: 33, max: 40, gpa: 1.6 },
  { min: 21, max: 32, gpa: 1.2 },
  { min: 0, max: 20, gpa: 0 },
];

module.exports = function mapToGPA(avg) {
  const match = gradeMapping.find((g) => avg >= g.min && avg <= g.max);
  return match ? match.gpa : 0;
};
