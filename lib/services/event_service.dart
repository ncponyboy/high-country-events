import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:http/http.dart' as http;
import '../models/event.dart';

class EventService extends ChangeNotifier {
  List<Event> events = [];
  bool isLoading = false;
  String? errorMessage;

  static const String _url =
      'https://raw.githubusercontent.com/ncponyboy/high-country-events/refs/heads/main/high_country_events.json';

  Future<void> fetchEvents() async {
    isLoading = true;
    errorMessage = null;
    notifyListeners();

    try {
      final response = await http.get(Uri.parse(_url)).timeout(
        const Duration(seconds: 20),
      );

      if (response.statusCode == 200) {
        final dynamic decoded = jsonDecode(response.body);
        List<dynamic> rawEvents;

        if (decoded is List) {
          rawEvents = decoded;
        } else if (decoded is Map<String, dynamic>) {
          rawEvents = (decoded['events'] as List<dynamic>?) ?? [];
        } else {
          rawEvents = [];
        }

        final now = DateTime.now().subtract(const Duration(hours: 2));
        events = rawEvents
            .whereType<Map<String, dynamic>>()
            .map(Event.fromJson)
            .where((e) {
              final d = e.dateObject;
              return d != null && d.isAfter(now);
            })
            .toList()
          ..sort((a, b) {
            final da = a.dateObject;
            final db = b.dateObject;
            if (da == null && db == null) return 0;
            if (da == null) return 1;
            if (db == null) return -1;
            return da.compareTo(db);
          });

        errorMessage = null;
      } else {
        errorMessage = 'Failed to load events (${response.statusCode}).';
      }
    } catch (e) {
      errorMessage = 'Could not load events. Check your connection.';
      debugPrint('EventService error: $e');
    } finally {
      isLoading = false;
      notifyListeners();
    }
  }

  /// All unique source names from current events list.
  List<String> get allSources {
    final sources = events.map((e) => e.source).toSet().toList()..sort();
    return sources;
  }
}
