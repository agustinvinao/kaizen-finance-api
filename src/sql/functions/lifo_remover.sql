CREATE FUNCTION filter_hhll(items float[], is_high bool)
  RETURNS float[]
AS $$
arr = []
for item in items:
	if len(arr) > 0:
		if is_high:
			while len(arr) > 0 and item > arr[-1]:
				arr.pop()
		else:
			while len(arr) > 0 and item < arr[-1]:
				arr.pop()
	arr.append(item)
return arr
$$ LANGUAGE plpython3u;



CREATE FUNCTION filter_hhll_positions(items float[], positions int[], is_high bool)
  RETURNS int[]
AS $$
arr = []
arr_positions = []
for idx, item in enumerate(items):
	if len(arr) > 0:
		if is_high:
			while len(arr) > 0 and item > arr[-1]:
				arr.pop()
				arr_positions.pop()
		else:
			while len(arr) > 0 and item < arr[-1]:
				arr.pop()
				arr_positions.pop()
	arr.append(item)
	arr_positions.append(item)
return arr_positions
$$ LANGUAGE plpython3u;



-- DROP FUNCTION filter_hhll(items float[], is_high bool)