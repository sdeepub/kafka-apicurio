package local.home.arpa.telemetry.spc;

import java.util.ArrayList;
import java.util.List;

/**
 * Western Electric / Nelson zone-test rules (the set AIAG's SPC manual is built on), evaluated
 * against a rolling window of a single device's own recent readings for one measurement.
 *
 * IMPORTANT LIMITATION: the center line and sigma below are recomputed from the same trailing
 * window being tested ("moving control limits"), not from a frozen Phase-I baseline study.
 * This is a reasonable real-time starting point, but a slow drift can partially chase itself
 * and dampen sensitivity to that same drift. Freezing the baseline periodically (or after an
 * operator confirms a stable run) is the natural next step, not implemented here.
 *
 * Requires at least MIN_POINTS in the window before evaluating anything (warm-up).
 */
public final class SpcEvaluator {

    public static final int MIN_POINTS = 15;
    public static final int WINDOW_CAPACITY = 25;

    public static class Violation {
        public final int rule;
        public final String description;
        Violation(int rule, String description) { this.rule = rule; this.description = description; }
    }

    private SpcEvaluator() {}

    public static List<Violation> evaluate(List<Double> window) {
        List<Violation> out = new ArrayList<>();
        int n = window.size();
        if (n < MIN_POINTS) return out; // still warming up

        double mean = mean(window);
        double sd = stddev(window, mean);
        if (sd == 0.0) return out; // no variation yet - zone tests are meaningless

        double ucl1 = mean + sd,  lcl1 = mean - sd;
        double ucl2 = mean + 2 * sd, lcl2 = mean - 2 * sd;
        double ucl3 = mean + 3 * sd, lcl3 = mean - 3 * sd;

        double latest = window.get(n - 1);

        // Rule 1: single point beyond 3 sigma
        if (latest > ucl3 || latest < lcl3) {
            out.add(new Violation(1, "Point beyond 3-sigma control limit"));
        }

        // Rule 2: 9 points in a row on the same side of the centerline
        List<Double> t9 = tail(window, 9);
        if (t9.size() == 9 && allSameSide(t9, mean)) {
            out.add(new Violation(2, "9 points in a row on the same side of centerline"));
        }

        // Rule 3: 6 points in a row steadily increasing or decreasing
        List<Double> t6 = tail(window, 6);
        if (t6.size() == 6 && isMonotonic(t6)) {
            out.add(new Violation(3, "6 points in a row steadily trending"));
        }

        // Rule 4: 14 points in a row alternating up and down
        List<Double> t14 = tail(window, 14);
        if (t14.size() == 14 && isAlternating(t14)) {
            out.add(new Violation(4, "14 points in a row alternating up/down"));
        }

        // Rule 5: 2 of 3 consecutive points beyond 2 sigma, same side
        List<Double> t3 = tail(window, 3);
        if (t3.size() == 3 && (countBeyond(t3, ucl2, true) >= 2 || countBeyond(t3, lcl2, false) >= 2)) {
            out.add(new Violation(5, "2 of 3 points beyond 2-sigma on the same side"));
        }

        // Rule 6: 4 of 5 consecutive points beyond 1 sigma, same side
        List<Double> t5 = tail(window, 5);
        if (t5.size() == 5 && (countBeyond(t5, ucl1, true) >= 4 || countBeyond(t5, lcl1, false) >= 4)) {
            out.add(new Violation(6, "4 of 5 points beyond 1-sigma on the same side"));
        }

        // Rule 7: 15 points in a row within 1 sigma of centerline (stratification)
        List<Double> t15 = tail(window, 15);
        if (t15.size() == 15 && t15.stream().allMatch(v -> v <= ucl1 && v >= lcl1)) {
            out.add(new Violation(7, "15 points in a row within 1-sigma (stratification)"));
        }

        // Rule 8: 8 points in a row with none within 1 sigma (mixture)
        List<Double> t8 = tail(window, 8);
        if (t8.size() == 8 && t8.stream().noneMatch(v -> v <= ucl1 && v >= lcl1)) {
            out.add(new Violation(8, "8 points in a row avoiding zone C (mixture pattern)"));
        }

        return out;
    }

    private static List<Double> tail(List<Double> list, int n) {
        int size = list.size();
        return size < n ? new ArrayList<>() : list.subList(size - n, size);
    }

    private static boolean allSameSide(List<Double> pts, double mean) {
        boolean allAbove = pts.stream().allMatch(v -> v > mean);
        boolean allBelow = pts.stream().allMatch(v -> v < mean);
        return allAbove || allBelow;
    }

    private static boolean isMonotonic(List<Double> pts) {
        boolean increasing = true, decreasing = true;
        for (int i = 1; i < pts.size(); i++) {
            if (pts.get(i) <= pts.get(i - 1)) increasing = false;
            if (pts.get(i) >= pts.get(i - 1)) decreasing = false;
        }
        return increasing || decreasing;
    }

    private static boolean isAlternating(List<Double> pts) {
        Boolean lastUp = null;
        for (int i = 1; i < pts.size(); i++) {
            boolean up = pts.get(i) > pts.get(i - 1);
            boolean down = pts.get(i) < pts.get(i - 1);
            if (!up && !down) return false; // a flat step breaks strict alternation
            if (lastUp != null && up == lastUp) return false;
            lastUp = up;
        }
        return true;
    }

    private static long countBeyond(List<Double> pts, double limit, boolean above) {
        return pts.stream().filter(v -> above ? v > limit : v < limit).count();
    }

    private static double mean(List<Double> pts) {
        double sum = 0;
        for (double v : pts) sum += v;
        return sum / pts.size();
    }

    private static double stddev(List<Double> pts, double mean) {
        if (pts.size() < 2) return 0.0;
        double sumSq = 0;
        for (double v : pts) sumSq += (v - mean) * (v - mean);
        return Math.sqrt(sumSq / (pts.size() - 1)); // sample stddev
    }
}
