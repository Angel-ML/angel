
## 1. exponential_decay
decayed_learning_rate = learning_rate *
decay_rate ^ (global_step / decay_steps)

## 2. piecewise_constant

## 3. polynomial_decay
  global_step = min(global_step, decay_steps)
  decayed_learning_rate = (learning_rate - end_learning_rate) *
                          (1 - global_step / decay_steps) ^ (power) +
                          end_learning_rate


  decay_steps = decay_steps * ceil(global_step / decay_steps)
  decayed_learning_rate = (learning_rate - end_learning_rate) *
                          (1 - global_step / decay_steps) ^ (power) +
                          end_learning_rate 
## 4. natural_exp_decay
decayed_learning_rate = learning_rate * exp(-decay_rate * global_step)


## 5. cosine_decay
  global_step = min(global_step, decay_steps)
  decayed = 0.5 * (1 + cos(pi * global_step / decay_steps))
  decayed_learning_rate = learning_rate * decayed

## 6. linear_cosine_decay
  global_step = min(global_step, decay_steps)
  linear_decay = (decay_steps - global_step) / decay_steps)
  cosine_decay = 0.5 * (
      1 + cos(pi * 2 * num_periods * global_step / decay_steps))
  decayed = (alpha + linear_decay) * cosine_decay + beta
  decayed_learning_rate = learning_rate * decayed


## 7. noisy_linear_cosine_decay 　
  global_step = min(global_step, decay_steps)
  linear_decay = (decay_steps - global_step) / decay_steps)
  cosine_decay = 0.5 * (
      1 + cos(pi * 2 * num_periods * global_step / decay_steps))
  decayed = (alpha + linear_decay + eps_t) * cosine_decay + beta
  decayed_learning_rate = learning_rate * decayed

## 8. inverse_time_decay
decayed_learning_rate = learning_rate / (1 + decay_rate * global_step / decay_step)

decayed_learning_rate = learning_rate / (1 + decay_rate * floor(global_step / decay_step))