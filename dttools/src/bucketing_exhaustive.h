/* Predict a value
 * @param prev_val previous value to consider, if any
 * @param s the relevant bucketing_state
 * @return the predicted value */
double bucketing_predict(double prev_val, bucketing_state* s);

/* Calculate the buckets from a bucketing state
 * @param the relevant bucketing state
 * @return 0 if success
 * @return 1 if failure */
int bucketing_get_buckets(bucketing_state *s);
